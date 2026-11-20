import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import {
  io as crearCliente,
  Socket as SocketCliente,
} from "socket.io-client";
import { crearServidorRealtime } from "../app";
import { PresenceTracker } from "../services/presence.service";
import {
  ClientToServerEvents,
  MensajeNuevoPayload,
  PresenceUpdatePayload,
  SalaErrorPayload,
  SalaUnidaPayload,
  ServerToClientEvents,
} from "../types/events";
import { MensajePersistido } from "../types/message";
import { RealtimeDependencies } from "../sockets";

type Cliente = SocketCliente<ServerToClientEvents, ClientToServerEvents>;

const salas = new Map([
  ["room-a", { id: "room-a", nombre: "Sala A", ownerUid: "owner-a" }],
  ["room-b", { id: "room-b", nombre: "Sala B", ownerUid: "owner-b" }],
]);

interface Harness {
  url: string;
  app: ReturnType<typeof crearServidorRealtime>;
  clients: Set<Cliente>;
  messages: MensajePersistido[];
  connect(token?: string): Cliente;
  close(): Promise<void>;
}

async function crearHarness(
  overrides: Partial<RealtimeDependencies> = {},
): Promise<Harness> {
  const messages: MensajePersistido[] = [];
  const dependencies: RealtimeDependencies = {
    async verifyIdToken(token) {
      const users: Record<string, { uid: string; name: string }> = {
        alice: { uid: "uid-alice", name: "Alice" },
        alice2: { uid: "uid-alice", name: "Alice" },
        bob: { uid: "uid-bob", name: "Bob" },
        guest: { uid: "uid-guest", name: "Guest" },
      };
      const user = users[token];
      if (user) return user;
      if (token === "expired") {
        throw Object.assign(new Error("expired token"), {
          code: "auth/id-token-expired",
        });
      }
      throw Object.assign(new Error("invalid token"), {
        code: "auth/invalid-id-token",
      });
    },
    async getRoom(roomId) {
      return salas.get(roomId) ?? null;
    },
    async saveMessage(input) {
      const message: MensajePersistido = {
        id: `message-${messages.length + 1}`,
        ...input,
        createdAt: new Date().toISOString(),
      };
      messages.push(message);
      return message;
    },
    ...overrides,
  };
  const app = crearServidorRealtime(dependencies, []);
  await new Promise<void>((resolve) => app.httpServer.listen(0, resolve));
  const address = app.httpServer.address();
  if (!address || typeof address === "string") {
    throw new Error("No se pudo obtener el puerto temporal del servidor");
  }

  const clients = new Set<Cliente>();
  const url = `http://127.0.0.1:${address.port}`;
  return {
    url,
    app,
    clients,
    messages,
    connect(token) {
      const client = crearCliente(url, {
          auth: token ? { token } : {},
          transports: ["websocket"],
          reconnection: false,
          forceNew: true,
        }) as Cliente;
      clients.add(client);
      return client;
    },
    async close() {
      for (const client of clients) client.disconnect();
      await new Promise<void>((resolve) => app.io.close(() => resolve()));
    },
  };
}

async function conectado(cliente: Cliente): Promise<void> {
  if (cliente.connected) return;
  await new Promise<void>((resolve, reject) => {
    cliente.once("connect", resolve);
    cliente.once("connect_error", reject);
  });
}

async function ingresar(cliente: Cliente, roomId: string): Promise<SalaUnidaPayload> {
  const respuesta = new Promise<SalaUnidaPayload>((resolve) => {
    cliente.once("room:joined", resolve);
  });
  cliente.emit("room:join", { roomId });
  return respuesta;
}

async function errorDeSala(
  cliente: Cliente,
  emitir: () => void,
): Promise<SalaErrorPayload> {
  const respuesta = new Promise<SalaErrorPayload>((resolve) => {
    cliente.once("room:error", resolve);
  });
  emitir();
  return respuesta;
}

async function presencia(
  cliente: Cliente,
  uid: string,
  status: "online" | "offline",
): Promise<PresenceUpdatePayload> {
  return new Promise<PresenceUpdatePayload>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`No llegó presencia ${status} para ${uid}`)),
      1500,
    );
    const listener = (payload: PresenceUpdatePayload) => {
      if (payload.uid !== uid || payload.status !== status) return;
      clearTimeout(timer);
      cliente.off("presence:update", listener);
      resolve(payload);
    };
    cliente.on("presence:update", listener);
  });
}

async function cerrarHarnesses(harnesses: Set<Harness>): Promise<void> {
  await Promise.all([...harnesses].map((harness) => harness.close()));
}

describe("servidor realtime", () => {
  const harnesses = new Set<Harness>();
  afterEach(async () => {
    await cerrarHarnesses(harnesses);
    harnesses.clear();
  });

  async function nuevoHarness(): Promise<Harness> {
    const harness = await crearHarness();
    harnesses.add(harness);
    return harness;
  }

  it("acepta tokens válidos y rechaza conexiones sin token o con token inválido", async () => {
    const harness = await nuevoHarness();

    const sinToken = harness.connect();
    const errorSinToken = new Promise<Error>((resolve) =>
      sinToken.once("connect_error", resolve),
    );
    assert.equal((await errorSinToken).message, "AUTH_REQUIRED");

    const invalido = harness.connect("invalid");
    const errorInvalido = new Promise<Error>((resolve) =>
      invalido.once("connect_error", resolve),
    );
    assert.equal((await errorInvalido).message, "INVALID_TOKEN");

    const expirado = harness.connect("expired");
    const errorExpirado = new Promise<Error>((resolve) =>
      expirado.once("connect_error", resolve),
    );
    assert.equal((await errorExpirado).message, "INVALID_TOKEN");

    const valido = harness.connect("alice");
    await conectado(valido);
    assert.equal(valido.connected, true);
  });

  it("permite el ingreso invitado a una sala existente y rechaza una inexistente", async () => {
    const harness = await nuevoHarness();
    const cliente = harness.connect("guest");
    await conectado(cliente);

    const sala = await ingresar(cliente, "room-a");
    assert.equal(sala.roomName, "Sala A");
    assert.deepEqual(sala.onlineUsers, [
      { uid: "uid-guest", displayName: "Guest" },
    ]);

    const error = await errorDeSala(cliente, () =>
      cliente.emit("room:join", { roomId: "missing-room" }),
    );
    assert.equal(error.code, "ROOM_NOT_FOUND");
  });

  it("rechaza mensajes de sockets fuera de la sala y contenido inválido", async () => {
    const harness = await nuevoHarness();
    const cliente = harness.connect("alice");
    await conectado(cliente);

    const fueraDeSala = await errorDeSala(cliente, () =>
      cliente.emit("message:send", {
        roomId: "room-a",
        content: "no autorizado",
      }),
    );
    assert.equal(fueraDeSala.code, "NOT_IN_ROOM");
    assert.equal(harness.messages.length, 0);

    await ingresar(cliente, "room-a");
    const vacio = await errorDeSala(cliente, () =>
      cliente.emit("message:send", { roomId: "room-a", content: "  " }),
    );
    assert.equal(vacio.code, "INVALID_CONTENT");

    const largo = await errorDeSala(cliente, () =>
      cliente.emit("message:send", {
        roomId: "room-a",
        content: "x".repeat(2001),
      }),
    );
    assert.equal(largo.code, "INVALID_CONTENT");
    assert.equal(harness.messages.length, 0);
  });

  it("persiste antes de emitir y distribuye solo dentro de la sala", async () => {
    const harness = await nuevoHarness();
    const emisor = harness.connect("alice");
    const colega = harness.connect("bob");
    const otraSala = harness.connect("guest");
    await Promise.all([conectado(emisor), conectado(colega), conectado(otraSala)]);
    await Promise.all([
      ingresar(emisor, "room-a"),
      ingresar(colega, "room-a"),
      ingresar(otraSala, "room-b"),
    ]);

    const eventos = [emisor, colega].map(
      (cliente) =>
        new Promise<MensajeNuevoPayload>((resolve) =>
          cliente.once("message:new", resolve),
        ),
    );
    let recibioOtraSala = false;
    const persistidoAlEmitir: boolean[] = [];
    for (const cliente of [emisor, colega]) {
      cliente.once("message:new", () => {
        persistidoAlEmitir.push(harness.messages.length === 1);
      });
    }
    otraSala.once("message:new", () => {
      recibioOtraSala = true;
    });

    emisor.emit("message:send", { roomId: "room-a", content: " Hola " });
    const mensajesRecibidos = await Promise.all(eventos);
    assert.equal(harness.messages.length, 1);
    assert.equal(mensajesRecibidos[0].senderUid, "uid-alice");
    assert.equal(mensajesRecibidos[0].senderName, "Alice");
    assert.equal(mensajesRecibidos[0].content, "Hola");
    assert.equal(mensajesRecibidos[0].id, harness.messages[0].id);
    assert.deepEqual(persistidoAlEmitir, [true, true]);

    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(recibioOtraSala, false);
  });

  it("mantiene presencia hasta que se desconecta el último socket de un usuario", async () => {
    const harness = await nuevoHarness();
    const observador = harness.connect("bob");
    const alice = harness.connect("alice");
    const aliceSegundaConexion = harness.connect("alice2");
    await Promise.all([
      conectado(observador),
      conectado(alice),
      conectado(aliceSegundaConexion),
    ]);
    await ingresar(observador, "room-a");

    const online = presencia(observador, "uid-alice", "online");
    await ingresar(alice, "room-a");
    await online;

    const sinCambio = new Promise<boolean>((resolve) => {
      const listener = (payload: PresenceUpdatePayload) => {
        if (payload.uid === "uid-alice") resolve(true);
      };
      observador.once("presence:update", listener);
      setTimeout(() => {
        observador.off("presence:update", listener);
        resolve(false);
      }, 100);
    });
    await ingresar(aliceSegundaConexion, "room-a");
    assert.equal(await sinCambio, false);

    const offline = presencia(observador, "uid-alice", "offline");
    alice.disconnect();
    await new Promise((resolve) => setTimeout(resolve, 100));
    aliceSegundaConexion.disconnect();
    assert.equal((await offline).status, "offline");
  });

  it("no anuncia un mensaje cuando falla su persistencia", async () => {
    const harness = await crearHarness({
      saveMessage: async () => {
        throw new Error("Firestore unavailable");
      },
    });
    harnesses.add(harness);
    const emisor = harness.connect("alice");
    const colega = harness.connect("bob");
    await Promise.all([conectado(emisor), conectado(colega)]);
    await Promise.all([ingresar(emisor, "room-a"), ingresar(colega, "room-a")]);

    let mensajeAnunciado = false;
    colega.once("message:new", () => {
      mensajeAnunciado = true;
    });
    const error = await errorDeSala(emisor, () =>
      emisor.emit("message:send", { roomId: "room-a", content: "No guardar" }),
    );
    assert.equal(error.code, "MESSAGE_PERSISTENCE_FAILED");
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(mensajeAnunciado, false);
  });

  it("limpia presencia al usar room:leave", async () => {
    const harness = await nuevoHarness();
    const observador = harness.connect("bob");
    const invitado = harness.connect("guest");
    await Promise.all([conectado(observador), conectado(invitado)]);
    await ingresar(observador, "room-a");
    const online = presencia(observador, "uid-guest", "online");
    await ingresar(invitado, "room-a");
    await online;

    const left = new Promise<{ roomId: string }>((resolve) =>
      invitado.once("room:left", resolve),
    );
    const offline = presencia(observador, "uid-guest", "offline");
    invitado.emit("room:leave", { roomId: "room-a" });
    assert.equal((await left).roomId, "room-a");
    assert.equal((await offline).status, "offline");
  });

  it("expone un endpoint de salud HTTP", async () => {
    const harness = await nuevoHarness();
    const response = await fetch(`${harness.url}/health`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { estado: "ok" });
  });

  it("incluye el tracker de presencia por proceso", () => {
    const presence = new PresenceTracker();
    assert.equal(
      presence.agregar("room-a", "socket-a", {
        uid: "uid-a",
        displayName: "A",
      }),
      true,
    );
    assert.equal(
      presence.agregar("room-a", "socket-b", {
        uid: "uid-a",
        displayName: "A",
      }),
      false,
    );
    assert.equal(presence.usuariosEnSala("room-a").length, 1);
    assert.equal(presence.quitar("room-a", "socket-a")?.quedoOffline, false);
    assert.equal(presence.quitarSocket("socket-b").length, 1);
    assert.equal(presence.usuariosEnSala("room-a").length, 0);
  });
});
