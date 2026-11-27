import { Server, Socket } from "socket.io";
import { crearMiddlewareSocketAuth, verificarSesionSocket } from "../middlewares/socketAuth";
import { PresenceTracker } from "../services/presence.service";
import { SalaVigilada, VigilanciaSalas } from "../services/room-watch.service";
import {
  ClientToServerEvents,
  InterServerEvents,
  ServerToClientEvents,
} from "../types/events";
import {
  EliminacionCuentaEnCursoError,
  PerfilEmisorNoEncontradoError,
  SalaNoEncontradaError,
} from "../types/domainErrors";
import { MensajePersistido } from "../types/message";
import { ObservarSala } from "../types/room";

const MAX_ROOM_ID_LENGTH = 128;
const MAX_MESSAGE_LENGTH = 2000;
const MAX_ADMISIONES_PENDIENTES = 8;
const MAX_ENVIOS_PENDIENTES = 8;
const PLAZO_ADMISION_MS = 5_000;

interface AdmisionPendiente {
  signal: AbortSignal;
  cancelada: Promise<undefined>;
  verificandoAuth: boolean;
  enAdaptador: boolean;
  sala?: SalaVigilada;
  liberarSala?: () => void;
  cancelar(): void;
}

export interface RealtimeDependencies {
  verifyIdToken: (token: string) => Promise<{ uid: string; name?: string }>;
  getRoom: (
    roomId: string,
  ) => Promise<{ id: string; nombre: string; ownerUid: string } | null>;
  watchRoom: ObservarSala;
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
    normalizado === "." || normalizado === ".." ||
    /^__[\s\S]*__$/.test(normalizado) ||
    /[/\u0000-\u001f\u007f]/.test(normalizado)
  ) {
    return null;
  }
  return normalizado;
}

/** Socket.IO reserva un canal privado que se llama igual que el socket. */
function canalSala(roomId: string): string {
  return `study-room:${roomId}`;
}

function emitirError(
  socket: RealtimeSocket,
  code: string,
  message: string,
  roomId?: string,
): void {
  socket.emit("room:error", { code, message, ...(roomId ? { roomId } : {}) });
}

async function verificarSesionActual(
  socket: RealtimeSocket,
  verificarToken: RealtimeDependencies["verifyIdToken"],
): Promise<void> {
  const usuario = await verificarSesionSocket(socket.handshake.auth?.token, verificarToken);
  if (usuario.uid !== socket.data.uid) throw new Error("INVALID_TOKEN");
}

function emitirErrorSesion(socket: RealtimeSocket, roomId: string, error: unknown): void {
  const code = error instanceof Error ? error.message : "AUTH_UNAVAILABLE";
  const message = code === "AUTH_UNAVAILABLE"
    ? "No se pudo verificar tu sesión. Intenta de nuevo."
    : "Tu sesión no es válida. Vuelve a conectarte con un token vigente.";
  emitirError(socket, code, message, roomId);
}

async function ingresarSala(
  socket: RealtimeSocket,
  roomId: string,
  dependencies: RealtimeDependencies,
  presence: PresenceTracker,
  vigilancia: VigilanciaSalas,
  admision: AdmisionPendiente,
): Promise<void> {
  try {
    await verificarSesionActual(socket, dependencies.verifyIdToken);
    if (admision.signal.aborted || !socket.connected) return;
  } catch (error) {
    if (admision.signal.aborted) return;
    emitirErrorSesion(socket, roomId, error);
    return;
  } finally {
    admision.verificandoAuth = false;
  }

  const { sala, liberar } = vigilancia.retener(roomId);
  admision.sala = sala;
  admision.liberarSala = liberar;
  try {
    let room: Awaited<ReturnType<RealtimeDependencies["getRoom"]>> | undefined;
    try {
      room = sala.cerrada ? null : await Promise.race([
        dependencies.getRoom(roomId),
        admision.cancelada,
      ]);
    } catch (error) {
      if (admision.signal.aborted) return;
      if (sala.cerrada) {
        emitirError(socket, "ROOM_NOT_FOUND", "La sala no existe.", roomId);
        return;
      }
      console.error("[Socket.IO] error consultando sala:", error);
      emitirError(socket, "ROOM_LOOKUP_FAILED", "No se pudo verificar la sala.", roomId);
      return;
    }
    if (admision.signal.aborted) return;
    if (!room) vigilancia.cerrar(roomId, sala);
    if (!room || sala.cerrada) {
      emitirError(socket, "ROOM_NOT_FOUND", "La sala no existe.", roomId);
      return;
    }

    if (!socket.connected) return;
    if (presence.tieneConexion(roomId, socket.id, socket.data.uid)) {
      socket.emit("room:joined", {
        roomId,
        roomName: room.nombre,
        onlineUsers: presence.usuariosEnSala(roomId),
      });
      return;
    }

    admision.enAdaptador = true;
    const canal = canalSala(roomId);
    await socket.join(canal);
    if (!socket.connected || admision.signal.aborted || sala.cerrada) {
      await socket.leave(canal);
      if (socket.connected && !admision.signal.aborted) {
        emitirError(socket, "ROOM_NOT_FOUND", "La sala no existe.", roomId);
      }
      return;
    }
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
      socket.to(canal).emit("presence:update", {
        roomId,
        ...identity,
        status: "online",
      });
    }
  } catch (error) {
    if (admision.signal.aborted) return;
    if (sala.cerrada) {
      emitirError(socket, "ROOM_NOT_FOUND", "La sala no existe.", roomId);
      return;
    }
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
  vigilancia: VigilanciaSalas,
): Promise<void> {
  const roomId = leerRoomId(payload);
  if (!roomId) {
    emitirError(socket, "INVALID_ROOM_ID", "El ID de sala no es válido.");
    return;
  }
  const sala = vigilancia.obtener(roomId);
  if (!sala || !presence.tieneConexion(roomId, socket.id, socket.data.uid)) {
    emitirError(
      socket,
      "NOT_IN_ROOM",
      "Este socket no pertenece a la sala.",
      roomId,
    );
    return;
  }

  try {
    await socket.leave(canalSala(roomId));
  } catch (error) {
    if (sala.cerrada) return;
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
  vigilancia.liberarSiVacia(roomId);
  socket.emit("room:left", { roomId });
  if (salida?.quedoOffline) {
    io.to(canalSala(roomId)).emit("presence:update", {
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
  vigilancia: VigilanciaSalas,
): Promise<void> {
  const data = leerObjeto(payload);
  const roomId = leerRoomId(data);
  if (!roomId) {
    emitirError(socket, "INVALID_ROOM_ID", "El ID de sala no es válido.");
    return;
  }
  const sala = vigilancia.obtener(roomId);
  const membresia = presence.obtenerMembresia(roomId, socket.id, socket.data.uid);
  if (!sala || !membresia) {
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

  try {
    await verificarSesionActual(socket, dependencies.verifyIdToken);
  } catch (error) {
    if (socket.connected && presence.obtenerMembresia(roomId, socket.id, socket.data.uid) === membresia) {
      emitirErrorSesion(socket, roomId, error);
    }
    return;
  }

  if (!socket.connected) return;
  if (sala.cerrada) {
    emitirError(socket, "ROOM_NOT_FOUND", "La sala no existe.", roomId);
    return;
  }
  const membresiaActual = presence.obtenerMembresia(roomId, socket.id, socket.data.uid);
  if (!membresiaActual) {
    emitirError(socket, "NOT_IN_ROOM", "Debes ingresar a la sala antes de enviar mensajes.", roomId);
    return;
  }
  // Un reingreso del mismo socket no autoriza el envío anterior.
  if (membresiaActual !== membresia) return;

  const { liberar } = vigilancia.retener(roomId);
  try {
    const message = await dependencies.saveMessage({
      roomId,
      senderUid: socket.data.uid,
      senderName: socket.data.displayName?.trim() || "Usuario",
      content,
    });
    if (!sala.cerrada) io.to(canalSala(roomId)).emit("message:new", message);
  } catch (error) {
    if (sala.cerrada) return;
    if (error instanceof SalaNoEncontradaError) {
      vigilancia.cerrar(roomId, sala);
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
    if (error instanceof PerfilEmisorNoEncontradoError) {
      emitirError(socket, "ACCOUNT_NOT_AVAILABLE", error.message, roomId);
      return;
    }
    console.error("[Socket.IO] no se pudo persistir el mensaje:", error);
    emitirError(
      socket,
      "MESSAGE_PERSISTENCE_FAILED",
      "No se pudo guardar el mensaje.",
      roomId,
    );
  } finally {
    liberar();
  }
}

export function configurarSockets(
  io: RealtimeServer,
  dependencies: RealtimeDependencies,
  presence: PresenceTracker,
): void {
  const vigilancia = new VigilanciaSalas(
    dependencies.watchRoom,
    (roomId) => presence.tieneUsuarios(roomId),
    (roomId) => {
      const canal = canalSala(roomId);
      for (const socketId of presence.quitarSala(roomId)) {
        const socket = io.sockets.sockets.get(socketId);
        if (socket?.rooms.has(canal)) socket.emit("room:deleted", { roomId });
      }
      io.in(canal).socketsLeave(canal);
    },
  );
  io.use(crearMiddlewareSocketAuth(dependencies.verifyIdToken));

  io.on("connection", (socket) => {
    const pendientes = new Map<string, AdmisionPendiente>();
    let enviosPendientes = 0;
    socket.on("room:join", (payload) => {
      const roomId = leerRoomId(payload);
      if (!roomId) {
        emitirError(socket, "INVALID_ROOM_ID", "El ID de sala no es válido.");
        return;
      }
      const anterior = pendientes.get(roomId);
      if (anterior && !anterior.signal.aborted) return;
      if (anterior || pendientes.size >= MAX_ADMISIONES_PENDIENTES) {
        emitirError(socket, "TOO_MANY_PENDING_JOINS", "Espera a que terminen tus ingresos pendientes.", roomId);
        return;
      }

      const control = new AbortController();
      const cancelada = new Promise<undefined>((resolve) => {
        control.signal.addEventListener("abort", () => resolve(undefined), { once: true });
      });
      let plazo: NodeJS.Timeout | undefined;
      const finalizar = () => {
        clearTimeout(plazo);
        plazo = undefined;
        admision.liberarSala?.();
        admision.liberarSala = undefined;
        // El plazo cancela el ingreso, pero Auth o el adaptador pueden seguir trabajando.
        if (!admision.verificandoAuth && !admision.enAdaptador && pendientes.get(roomId) === admision) {
          pendientes.delete(roomId);
        }
      };
      const admision: AdmisionPendiente = {
        signal: control.signal,
        cancelada,
        verificandoAuth: true,
        enAdaptador: false,
        cancelar() {
          control.abort();
          finalizar();
        },
      };
      plazo = setTimeout(() => {
        admision.cancelar();
        if (admision.sala?.cerrada) {
          emitirError(socket, "ROOM_NOT_FOUND", "La sala no existe.", roomId);
        } else {
          emitirError(socket, "ROOM_JOIN_TIMEOUT", "Se agotó el tiempo para ingresar a la sala.", roomId);
        }
      }, PLAZO_ADMISION_MS);
      pendientes.set(roomId, admision);
      void ingresarSala(socket, roomId, dependencies, presence, vigilancia, admision)
        .finally(() => {
          // Si el adaptador seguía uniendo el socket al vencer el plazo, el
          // intento retiene su turno hasta que pueda revertir esa membresía.
          admision.enAdaptador = false;
          finalizar();
        });
    });
    socket.on("room:leave", (payload) => {
      void salirSala(socket, payload, presence, io, vigilancia);
    });
    socket.on("message:send", (payload) => {
      if (enviosPendientes >= MAX_ENVIOS_PENDIENTES) {
        emitirError(socket, "TOO_MANY_PENDING_MESSAGES", "Espera a que terminen tus envíos pendientes.", leerRoomId(payload) ?? undefined);
        return;
      }
      enviosPendientes++;
      void enviarMensaje(socket, payload, dependencies, presence, io, vigilancia)
        .finally(() => { enviosPendientes--; });
    });
    socket.on("disconnect", () => {
      for (const admision of pendientes.values()) admision.cancelar();
      for (const salida of presence.quitarSocket(socket.id)) {
        vigilancia.liberarSiVacia(salida.roomId);
        io.to(canalSala(salida.roomId)).emit("presence:update", {
          roomId: salida.roomId,
          ...salida.usuario,
          status: "offline",
        });
      }
    });
  });
}
