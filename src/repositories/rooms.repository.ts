import { Timestamp } from "firebase-admin/firestore";
import { obtenerFirestore } from "../config/firebase";
import { ObservarSala, SalaDocumento } from "../types/room";

const ROOMS = "rooms";

export interface SalaPersistida extends SalaDocumento {
  id: string;
}

export async function obtenerSala(
  roomId: string,
): Promise<SalaPersistida | null> {
  const snapshot = await obtenerFirestore().collection(ROOMS).doc(roomId).get();
  if (!snapshot.exists || snapshot.get("closingAt") !== undefined) return null;

  const documento = snapshot.data();
  if (
    !documento ||
    typeof documento.nombre !== "string" ||
    typeof documento.ownerUid !== "string" ||
    !(documento.createdAt instanceof Timestamp)
  ) {
    throw new Error(`El documento rooms/${roomId} no tiene el formato esperado`);
  }

  return {
    id: snapshot.id,
    nombre: documento.nombre,
    ownerUid: documento.ownerUid,
    createdAt: documento.createdAt,
  };
}

export const observarSala: ObservarSala = (roomId, alCambiar, alFallar) =>
  obtenerFirestore().collection(ROOMS).doc(roomId).onSnapshot(
    (snapshot) => alCambiar(snapshot.exists && snapshot.get("closingAt") === undefined),
    alFallar,
  );
