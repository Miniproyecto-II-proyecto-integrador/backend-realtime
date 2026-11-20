import { Server, Socket } from "socket.io";
import { crearMiddlewareSocketAuth } from "../middlewares/socketAuth";
import { PresenceTracker } from "../services/presence.service";
import {
  ClientToServerEvents,
  InterServerEvents,
  ServerToClientEvents,
} from "../types/events";
import {
  EliminacionCuentaEnCursoError,
  SalaNoEncontradaError,
} from "../types/domainErrors";
import { MensajePersistido } from "../types/message";

const MAX_ROOM_ID_LENGTH = 128;
const MAX_MESSAGE_LENGTH = 2000;

export interface RealtimeDependencies {
  verifyIdToken: (token: string) => Promise<{ uid: string; name?: string }>;
  getRoom: (
    roomId: string,
  ) => Promise<{ id: string; nombre: string; ownerUid: string } | null>;
  saveMessage: (input: {
    roomId: string;
    senderUid: string;
    senderName: string;
    content: string;
  }) => Promise<MensajePersistido>;
}

type RealtimeServer = Server<
  ClientToServerEvents,
  ServerToClientEvents,
  InterServerEvents
>;
type RealtimeSocket = Socket<
  ClientToServerEvents,
  ServerToClientEvents,
  InterServerEvents
>;

function leerObjeto(valor: unknown): Record<string, unknown> | null {
  return typeof valor === "object" && valor !== null
    ? (valor as Record<string, unknown>)
    : null;
}

function leerRoomId(payload: unknown): string | null {
  const roomId = leerObjeto(payload)?.roomId;
  if (typeof roomId !== "string") return null;
  const normalizado = roomId.trim();
  if (
    normalizado.length === 0 ||
    normalizado.length > MAX_ROOM_ID_LENGTH ||
    normalizado.includes("/")
  ) {
    return null;
  }
  return normalizado;
}

function emitirError(
  socket: RealtimeSocket,
  code: string,
  message: string,
  roomId?: string,
): void {
  socket.emit("room:error", { code, message, ...(roomId ? { roomId } : {}) });
}

async function ingresarSala(
  socket: RealtimeSocket,
  payload: unknown,
  dependencies: RealtimeDependencies,
  presence: PresenceTracker,
): Promise<void> {
  const roomId = leerRoomId(payload);
  if (!roomId) {
    emitirError(socket, "INVALID_ROOM_ID", "El ID de sala no es válido.");
    return;
  }

  let room: Awaited<ReturnType<RealtimeDependencies["getRoom"]>>;
  try {
    room = await dependencies.getRoom(roomId);
  } catch (error) {
    console.error("[Socket.IO] error consultando sala:", error);
    emitirError(
      socket,
      "ROOM_LOOKUP_FAILED",
      "No se pudo verificar la sala.",
      roomId,
    );
    return;
  }

  if (!room) {
    emitirError(socket, "ROOM_NOT_FOUND", "La sala no existe.", roomId);
    return;
  }

  if (socket.rooms.has(roomId)) {
    socket.emit("room:joined", {
      roomId,
      roomName: room.nombre,
      onlineUsers: presence.usuariosEnSala(roomId),
    });
    return;
  }

  if (!socket.connected) return;
  try {
    await socket.join(roomId);
    if (!socket.connected) return;
    const identity = {
      uid: socket.data.uid,
      displayName: socket.data.displayName ?? "Usuario",
    };
    const becameOnline = presence.agregar(roomId, socket.id, identity);
    socket.emit("room:joined", {
      roomId,
      roomName: room.nombre,
      onlineUsers: presence.usuariosEnSala(roomId),
    });
    if (becameOnline) {
      socket.to(roomId).emit("presence:update", {
        roomId,
        ...identity,
        status: "online",
      });
    }
  } catch (error) {
    console.error("[Socket.IO] error ingresando a sala:", error);
    emitirError(
      socket,
      "ROOM_JOIN_FAILED",
      "No se pudo ingresar a la sala.",
      roomId,
    );
  }
}

async function salirSala(
  socket: RealtimeSocket,
  payload: unknown,
  presence: PresenceTracker,
  io: RealtimeServer,
): Promise<void> {
  const roomId = leerRoomId(payload);
  if (!roomId) {
    emitirError(socket, "INVALID_ROOM_ID", "El ID de sala no es válido.");
    return;
  }
  if (!presence.tieneConexion(roomId, socket.id, socket.data.uid)) {
    emitirError(
      socket,
      "NOT_IN_ROOM",
      "Este socket no pertenece a la sala.",
      roomId,
    );
    return;
  }

  try {
    await socket.leave(roomId);
  } catch (error) {
    console.error("[Socket.IO] error saliendo de sala:", error);
    emitirError(
      socket,
      "ROOM_LEAVE_FAILED",
      "No se pudo salir de la sala.",
      roomId,
    );
    return;
  }

  const salida = presence.quitar(roomId, socket.id);
  socket.emit("room:left", { roomId });
  if (salida?.quedoOffline) {
    io.to(roomId).emit("presence:update", {
      roomId,
      ...salida.usuario,
      status: "offline",
    });
  }
}

async function enviarMensaje(
  socket: RealtimeSocket,
  payload: unknown,
  dependencies: RealtimeDependencies,
  presence: PresenceTracker,
  io: RealtimeServer,
): Promise<void> {
  const data = leerObjeto(payload);
  const roomId = leerRoomId(data);
  if (!roomId) {
    emitirError(socket, "INVALID_ROOM_ID", "El ID de sala no es válido.");
    return;
  }
  if (!presence.tieneConexion(roomId, socket.id, socket.data.uid)) {
    emitirError(
      socket,
      "NOT_IN_ROOM",
      "Debes ingresar a la sala antes de enviar mensajes.",
      roomId,
    );
    return;
  }

  const content =
    typeof data?.content === "string" ? data.content.trim() : "";
  if (content.length === 0 || content.length > MAX_MESSAGE_LENGTH) {
    emitirError(
      socket,
      "INVALID_CONTENT",
      `El mensaje debe tener entre 1 y ${MAX_MESSAGE_LENGTH} caracteres.`,
      roomId,
    );
    return;
  }

  let message: MensajePersistido;
  try {
    message = await dependencies.saveMessage({
      roomId,
      senderUid: socket.data.uid,
      senderName: socket.data.displayName?.trim() || "Usuario",
      content,
    });
  } catch (error) {
    if (error instanceof SalaNoEncontradaError) {
      emitirError(socket, "ROOM_NOT_FOUND", error.message, roomId);
      return;
    }
    if (error instanceof EliminacionCuentaEnCursoError) {
      emitirError(
        socket,
        "ACCOUNT_DELETION_IN_PROGRESS",
        error.message,
        roomId,
      );
      return;
    }
    console.error("[Socket.IO] no se pudo persistir el mensaje:", error);
    emitirError(
      socket,
      "MESSAGE_PERSISTENCE_FAILED",
      "No se pudo guardar el mensaje.",
      roomId,
    );
    return;
  }

  io.to(roomId).emit("message:new", message);
}

export function configurarSockets(
  io: RealtimeServer,
  dependencies: RealtimeDependencies,
  presence: PresenceTracker,
): void {
  io.use(crearMiddlewareSocketAuth(dependencies.verifyIdToken));

  io.on("connection", (socket) => {
    socket.on("room:join", (payload) => {
      void ingresarSala(socket, payload, dependencies, presence);
    });
    socket.on("room:leave", (payload) => {
      void salirSala(socket, payload, presence, io);
    });
    socket.on("message:send", (payload) => {
      void enviarMensaje(socket, payload, dependencies, presence, io);
    });
    socket.on("disconnect", () => {
      for (const salida of presence.quitarSocket(socket.id)) {
        io.to(salida.roomId).emit("presence:update", {
          roomId: salida.roomId,
          ...salida.usuario,
          status: "offline",
        });
      }
    });
  });
}
