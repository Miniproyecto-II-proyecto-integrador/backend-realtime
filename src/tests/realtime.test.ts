import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { setTimeout as esperar } from "node:timers/promises";
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
import { PerfilEmisorNoEncontradoError, SalaNoEncontradaError } from "../types/domainErrors";

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
  suscripciones: Map<string, Suscripcion[]>;
  cerrarSala(id: string, notificar?: boolean): void;
  connect(token?: string): Cliente;
  close(): Promise<void>;
}

interface Suscripcion {
  activa: boolean;
  notificar(activa: boolean): void;
  fallar(error: Error): void;
}

async function crearHarness(
  overrides: Partial<RealtimeDependencies> = {},
): Promise<Harness> {
  const messages: MensajePersistido[] = [];
  const cerradas = new Set<string>();
  const suscripciones = new Map<string, Suscripcion[]>();
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
      return cerradas.has(roomId) ? null : salas.get(roomId) ?? null;
    },
    watchRoom(roomId, alCambiar, alFallar) {
      const suscripcion = { activa: true, notificar: alCambiar, fallar: alFallar };
      const anteriores = suscripciones.get(roomId) ?? [];
      anteriores.push(suscripcion);
      suscripciones.set(roomId, anteriores);
      queueMicrotask(() => {
        if (suscripcion.activa) alCambiar(salas.has(roomId) && !cerradas.has(roomId));
      });
      return () => { suscripcion.activa = false; };
    },
    async saveMessage(input) {
      if (cerradas.has(input.roomId)) throw new SalaNoEncontradaError();
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
    suscripciones,
    cerrarSala(roomId, notificar = true) {
      cerradas.add(roomId);
      if (notificar) {
        for (const suscripcion of suscripciones.get(roomId) ?? []) {
          if (suscripcion.activa) suscripcion.notificar(false);
        }
      }
    },
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

function diferir<T>() {
  let resolver!: (value: T) => void;
  const promesa = new Promise<T>((resolve) => { resolver = resolve; });
  return { promesa, resolver };
}

function salaEliminada(cliente: Cliente): Promise<{ roomId: string }> {
  return new Promise((resolve) => cliente.once("room:deleted", resolve));
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

  it("notifica el cierre solo a los miembros y conserva sus otras salas", async () => {
    const harness = await nuevoHarness();
    const alice = harness.connect("alice");
    const bob = harness.connect("bob");
    const invitado = harness.connect("guest");
    await Promise.all([alice, bob, invitado].map(conectado));
    await ingresar(alice, "room-a");
    await ingresar(bob, "room-a");
    await ingresar(alice, "room-b");
    await ingresar(invitado, "room-b");
    const aliceId = alice.id!;
    const bobId = bob.id!;
    const recibidos: string[] = [];
    alice.on("room:deleted", ({ roomId }) => recibidos.push(roomId));
    invitado.on("room:deleted", ({ roomId }) => recibidos.push(`invitado:${roomId}`));
    const avisos = Promise.all([salaEliminada(alice), salaEliminada(bob)]);

    const suscripciones = harness.suscripciones.get("room-a")!;
    assert.equal(suscripciones.length, 1);
    harness.cerrarSala("room-a");
    suscripciones[0].notificar(false);
    assert.deepEqual(await avisos, [{ roomId: "room-a" }, { roomId: "room-a" }]);
    assert.equal(suscripciones[0].activa, false);
    assert.deepEqual(harness.app.presence.usuariosEnSala("room-a"), []);
    assert.equal(harness.app.io.sockets.adapter.rooms.has("room-a"), false);
    assert.ok(harness.app.io.sockets.sockets.get(aliceId)?.rooms.has("room-b"));
    assert.ok(harness.app.io.sockets.sockets.get(bobId)?.connected);

    bob.disconnect();
    bob.connect();
    await conectado(bob);
    const error = await errorDeSala(bob, () => bob.emit("room:join", { roomId: "room-a" }));
    assert.equal(error.code, "ROOM_NOT_FOUND");
    await Promise.all([ingresar(alice, "room-b"), ingresar(invitado, "room-b")]);
    assert.deepEqual(recibidos, ["room-a"]);
  });

  it("revierte un ingreso pendiente si se observa el cierre antes de confirmarlo", async (t) => {
    const harness = await nuevoHarness();
    const anfitrion = harness.connect("alice");
    const invitado = harness.connect("guest");
    await Promise.all([conectado(anfitrion), conectado(invitado)]);
    await ingresar(anfitrion, "room-a");

    const ingresoEnCurso = diferir<void>();
    const continuarIngreso = diferir<void>();
    const adapter = harness.app.io.sockets.adapter;
    const addAll = adapter.addAll.bind(adapter);
    const invitadoId = invitado.id;
    t.mock.method(adapter, "addAll", (id: string, rooms: Set<string>) => {
      // Introducir el socket en el canal antes de completar la admisión.
      addAll(id, rooms);
      if (id === invitadoId && rooms.has("room-a")) {
        ingresoEnCurso.resolver();
        return continuarIngreso.promesa;
      }
    });

    const eventosInvitado: string[] = [];
    invitado.on("room:joined", () => eventosInvitado.push("joined"));
    invitado.on("room:deleted", () => eventosInvitado.push("deleted"));
    const rechazo = errorDeSala(invitado, () => invitado.emit("room:join", { roomId: "room-a" }));
    await ingresoEnCurso.promesa;
    const aviso = salaEliminada(anfitrion);
    harness.cerrarSala("room-a");
    continuarIngreso.resolver();

    assert.equal((await rechazo).code, "ROOM_NOT_FOUND");
    assert.deepEqual(await aviso, { roomId: "room-a" });
    assert.deepEqual(eventosInvitado, []);
    assert.equal(adapter.rooms.has("room-a"), false);
    assert.deepEqual(harness.app.presence.usuariosEnSala("room-a"), []);
  });

  it("observa la sala desde el primer ingreso pendiente y libera el listener al desconectar", async () => {
    const consultaEnCurso = diferir<void>();
    const consulta = diferir<Awaited<ReturnType<RealtimeDependencies["getRoom"]>>>();
    const harness = await crearHarness({
      getRoom() {
        consultaEnCurso.resolver();
        return consulta.promesa;
      },
    });
    harnesses.add(harness);
    const alice = harness.connect("alice");
    await conectado(alice);
    const socket = harness.app.io.sockets.sockets.get(alice.id!)!;
    alice.emit("room:join", { roomId: "room-a" });
    await consultaEnCurso.promesa;

    const suscripciones = harness.suscripciones.get("room-a")!;
    assert.equal(suscripciones.length, 1);
    assert.equal(suscripciones[0].activa, true);
    assert.equal(harness.app.presence.tieneUsuarios("room-a"), false);
    const desconectado = new Promise((resolve) => socket.once("disconnect", resolve));
    alice.disconnect();
    await desconectado;
    assert.equal(suscripciones[0].activa, false);
    consulta.resolver(salas.get("room-a")!);
  });

  it("deduplica, limita y cancela admisiones al vencer su plazo", async (t) => {
    const consulta = diferir<Awaited<ReturnType<RealtimeDependencies["getRoom"]>>>();
    const getRoom = t.mock.fn(() => consulta.promesa);
    const activos = new Set<string>();
    const harness = await crearHarness({
      getRoom,
      watchRoom(roomId) {
        activos.add(roomId);
        return () => { activos.delete(roomId); };
      },
    });
    harnesses.add(harness);
    const alice = harness.connect("alice");
    await conectado(alice);
    t.mock.timers.enable({ apis: ["setTimeout"] });

    alice.emit("room:join", { roomId: "room-a" });
    alice.emit("room:join", { roomId: "room-a" });
    for (let i = 1; i < 8; i++) alice.emit("room:join", { roomId: `pendiente-${i}` });
    const limite = await errorDeSala(alice, () => alice.emit("room:join", { roomId: "exceso" }));
    assert.equal(limite.code, "TOO_MANY_PENDING_JOINS");
    assert.equal(getRoom.mock.callCount(), 8);
    assert.equal(activos.size, 8);
    assert.equal(harness.app.io.sockets.sockets.get(alice.id!)?.listenerCount("disconnect"), 1);

    const vencidos = diferir<void>();
    const errores: string[] = [];
    alice.on("room:error", ({ code }) => {
      errores.push(code);
      if (errores.length === 8) vencidos.resolver();
    });
    t.mock.timers.tick(5_000);
    await vencidos.promesa;
    t.mock.timers.reset();
    assert.deepEqual(errores, Array(8).fill("ROOM_JOIN_TIMEOUT"));
    assert.equal(activos.size, 0);

    const confirmaciones: string[] = [];
    alice.on("room:joined", ({ roomId }) => confirmaciones.push(roomId));
    consulta.resolver(salas.get("room-a")!);
    await ingresar(alice, "room-a");
    assert.deepEqual(confirmaciones, ["room-a"]);
    assert.equal(getRoom.mock.callCount(), 9);
  });

  it("rechaza escrituras durante el cierre aunque el snapshot todavía no llegó", async () => {
    const harness = await nuevoHarness();
    const alice = harness.connect("alice");
    await conectado(alice);
    await ingresar(alice, "room-a");
    harness.cerrarSala("room-a", false);
    const aviso = salaEliminada(alice);
    const error = await errorDeSala(alice, () => {
      alice.emit("message:send", { roomId: "room-a", content: "Mensaje tardío" });
    });
    assert.equal(error.code, "ROOM_NOT_FOUND");
    assert.deepEqual(await aviso, { roomId: "room-a" });
    assert.deepEqual(harness.messages, []);
  });

  it("suprime la emisión de un mensaje cuyo guardado se resolvió después de observar el cierre", async (t) => {
    const guardadoEnCurso = diferir<Parameters<RealtimeDependencies["saveMessage"]>[0]>();
    const terminarGuardado = diferir<MensajePersistido>();
    const harness = await crearHarness({
      saveMessage(input) {
        guardadoEnCurso.resolver(input);
        return terminarGuardado.promesa;
      },
    });
    harnesses.add(harness);
    const alice = harness.connect("alice");
    await conectado(alice);
    await ingresar(alice, "room-a");
    const broadcast = t.mock.method(harness.app.io.sockets.adapter, "broadcast");
    const mensajesRecibidos: MensajeNuevoPayload[] = [];
    alice.on("message:new", (mensaje) => mensajesRecibidos.push(mensaje));
    alice.emit("message:send", { roomId: "room-a", content: "Hola" });
    const input = await guardadoEnCurso.promesa;
    const aviso = salaEliminada(alice);
    harness.cerrarSala("room-a");
    await aviso;

    terminarGuardado.resolver({ ...input, id: "uno", createdAt: new Date().toISOString() });
    await ingresar(alice, "room-b");
    assert.deepEqual(mensajesRecibidos, []);
    assert.ok(broadcast.mock.calls.every(({ arguments: [packet] }) => packet.data[0] !== "message:new"));
  });

  it("publica un mensaje persistido si la sala quedó vacía y volvió a tener miembros durante el envío", async () => {
    const guardadoEnCurso = diferir<Parameters<RealtimeDependencies["saveMessage"]>[0]>();
    const terminarGuardado = diferir<MensajePersistido>();
    const harness = await crearHarness({
      saveMessage(input) {
        guardadoEnCurso.resolver(input);
        return terminarGuardado.promesa;
      },
    });
    harnesses.add(harness);
    const alice = harness.connect("alice");
    const bob = harness.connect("bob");
    await Promise.all([conectado(alice), conectado(bob)]);
    await ingresar(alice, "room-a");
    alice.emit("message:send", { roomId: "room-a", content: "Sigue activa" });
    const input = await guardadoEnCurso.promesa;
    const salida = new Promise((resolve) => alice.once("room:left", resolve));
    alice.emit("room:leave", { roomId: "room-a" });
    await salida;

    const suscripciones = harness.suscripciones.get("room-a")!;
    assert.equal(suscripciones[0].activa, true);
    await ingresar(bob, "room-a");
    const recibido = new Promise<MensajeNuevoPayload>((resolve) => bob.once("message:new", resolve));
    terminarGuardado.resolver({ ...input, id: "uno", createdAt: new Date().toISOString() });
    assert.equal((await recibido).id, "uno");
    const salidaFinal = new Promise((resolve) => bob.once("room:left", resolve));
    bob.emit("room:leave", { roomId: "room-a" });
    await salidaFinal;
    assert.equal(suscripciones.length, 1);
    assert.equal(suscripciones[0].activa, false);
  });

  it("confirma una salida exitosa que coincide con el cierre de la sala", async (t) => {
    const harness = await nuevoHarness();
    const alice = harness.connect("alice");
    await conectado(alice);
    await ingresar(alice, "room-a");
    const salidaEnCurso = diferir<void>();
    const terminarSalida = diferir<void>();
    const adapter = harness.app.io.sockets.adapter;
    const del = adapter.del.bind(adapter);
    t.mock.method(adapter, "del", (id: string, roomId: string) => {
      del(id, roomId);
      if (roomId === "room-a") {
        salidaEnCurso.resolver();
        return terminarSalida.promesa;
      }
    });
    const salida = new Promise((resolve) => alice.once("room:left", resolve));
    alice.emit("room:leave", { roomId: "room-a" });
    await salidaEnCurso.promesa;
    harness.cerrarSala("room-a");
    terminarSalida.resolver();
    assert.deepEqual(await salida, { roomId: "room-a" });
    assert.deepEqual(harness.app.presence.usuariosEnSala("room-a"), []);
  });

  it("rechaza mensajes de un socket que conservó su conexión tras eliminar el perfil", async () => {
    const harness = await crearHarness({
      async saveMessage() { throw new PerfilEmisorNoEncontradoError(); },
    });
    harnesses.add(harness);
    const alice = harness.connect("alice");
    await conectado(alice);
    await ingresar(alice, "room-a");
    const error = await errorDeSala(alice, () => {
      alice.emit("message:send", { roomId: "room-a", content: "Cuenta eliminada" });
    });
    assert.equal(error.code, "ACCOUNT_NOT_AVAILABLE");
    assert.deepEqual(harness.messages, []);
  });

  it("recupera un listener fallido e ignora callbacks obsoletos", async () => {
    const harness = await nuevoHarness();
    const alice = harness.connect("alice");
    await conectado(alice);
    await ingresar(alice, "room-a");
    const suscripciones = harness.suscripciones.get("room-a")!;
    const anterior = suscripciones[0];
    anterior.fallar(new Error("Listener fallido simulado"));
    anterior.notificar(false);
    assert.equal(anterior.activa, false);
    assert.equal(harness.app.presence.usuariosEnSala("room-a").length, 1);

    const aviso = salaEliminada(alice);
    harness.cerrarSala("room-a", false);
    assert.deepEqual(await aviso, { roomId: "room-a" });
    assert.equal(suscripciones.length, 2);
    assert.equal(suscripciones[1].activa, false);
    assert.deepEqual(harness.app.presence.usuariosEnSala("room-a"), []);
  });

  it("cancela el reintento del listener si sale el último miembro", async () => {
    const harness = await nuevoHarness();
    const alice = harness.connect("alice");
    await conectado(alice);
    await ingresar(alice, "room-a");
    const suscripciones = harness.suscripciones.get("room-a")!;
    suscripciones[0].fallar(new Error("Listener fallido simulado"));
    const salida = new Promise((resolve) => alice.once("room:left", resolve));
    alice.emit("room:leave", { roomId: "room-a" });
    await salida;
    await esperar(150);
    assert.equal(suscripciones.length, 1);
    assert.equal(suscripciones[0].activa, false);
  });

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
