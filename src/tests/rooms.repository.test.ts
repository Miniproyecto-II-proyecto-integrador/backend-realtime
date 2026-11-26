import assert from "node:assert/strict";
import { it } from "node:test";
import firebaseApp = require("firebase-admin/app");
import firestore = require("firebase-admin/firestore");
import { guardarMensaje } from "../repositories/messages.repository";
import { observarSala, obtenerSala } from "../repositories/rooms.repository";
import {
  EliminacionCuentaEnCursoError, PerfilEmisorNoEncontradoError, SalaNoEncontradaError,
} from "../types/domainErrors";
import { SalaDocumento } from "../types/room";

it("el repositorio deja de admitir ingresos y escrituras si closingAt existe", async (t) => {
  const fecha = firestore.Timestamp.fromMillis(1000);
  let sala: SalaDocumento | undefined = { nombre: "Sala A", ownerUid: "host", createdAt: fecha };
  const salaActiva = sala;
  let perfilExiste = true;
  let eliminandoCuenta = false;
  let reintentar: (() => void) | undefined;
  const cancelar = t.mock.fn();
  const crearMensaje = t.mock.fn();

  function snapshot() {
    return {
      id: "room-a",
      exists: sala !== undefined,
      get: (campo: keyof SalaDocumento) => sala?.[campo],
      data: () => sala,
    };
  }
  let cambio!: (documento: ReturnType<typeof snapshot>) => void;
  const salaRef = {
    get: async () => snapshot(),
    onSnapshot(alCambiar: typeof cambio) {
      cambio = alCambiar;
      return cancelar;
    },
    collection: () => ({ doc: () => ({ id: "message-a" }) }),
  };
  const appFicticia = { name: "demo-test" } as firebaseApp.App;
  const perfilRef = {};
  t.mock.getter(firebaseApp, "getApps", () => () => [appFicticia]);
  t.mock.getter(firebaseApp, "getApp", () => () => appFicticia);
  t.mock.method(firestore, "getFirestore", () => ({
    collection: (nombre: string) => ({
      doc: () => nombre === "rooms" ? salaRef : nombre === "users" ? perfilRef : {},
    }),
    async runTransaction(operacion: (tx: object) => Promise<unknown>) {
      while (true) {
        const escrituras: unknown[][] = [];
        const resultado = await operacion({
          get: async (referencia: object) => referencia === salaRef
            ? snapshot()
            : { exists: referencia === perfilRef ? perfilExiste : eliminandoCuenta },
          create: (...args: unknown[]) => escrituras.push(args),
        });
        if (reintentar) {
          const cambiarEstado = reintentar;
          reintentar = undefined;
          cambiarEstado();
          continue;
        }
        escrituras.forEach((args) => crearMensaje(...args));
        return resultado;
      }
    },
  }));

  const estados: boolean[] = [];
  const salir = observarSala(
    "room-a",
    (estado) => estados.push(estado),
    (error) => assert.fail(String(error)),
  );
  const mensaje = { senderUid: "guest", senderName: "Invitado", content: "Hola" };
  cambio(snapshot());
  assert.equal((await obtenerSala("room-a"))?.id, "room-a");
  assert.equal((await guardarMensaje("room-a", mensaje)).id, "message-a");

  sala = { ...sala!, closingAt: fecha };
  cambio(snapshot());
  assert.equal(await obtenerSala("room-a"), null);
  await assert.rejects(guardarMensaje("room-a", mensaje), SalaNoEncontradaError);

  sala = undefined;
  cambio(snapshot());
  assert.equal(await obtenerSala("room-a"), null);
  await assert.rejects(guardarMensaje("room-a", mensaje), SalaNoEncontradaError);
  assert.deepEqual(estados, [true, false, false]);
  assert.equal(crearMensaje.mock.callCount(), 1);
  salir();
  assert.equal(cancelar.mock.callCount(), 1);

  sala = salaActiva;
  reintentar = () => { sala = { ...salaActiva, closingAt: fecha }; };
  await assert.rejects(guardarMensaje("room-a", mensaje), SalaNoEncontradaError);

  sala = salaActiva;
  reintentar = () => { eliminandoCuenta = true; };
  await assert.rejects(guardarMensaje("room-a", mensaje), EliminacionCuentaEnCursoError);

  eliminandoCuenta = false;
  reintentar = () => { perfilExiste = false; };
  await assert.rejects(guardarMensaje("room-a", mensaje), PerfilEmisorNoEncontradoError);
  await assert.rejects(guardarMensaje("room-a", mensaje), PerfilEmisorNoEncontradoError);
  assert.equal(crearMensaje.mock.callCount(), 1);
});
