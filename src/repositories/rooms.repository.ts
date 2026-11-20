import { Timestamp } from "firebase-admin/firestore";
import { obtenerFirestore } from "../config/firebase";
import { SalaDocumento } from "../types/room";

const ROOMS = "rooms";

export interface SalaPersistida extends SalaDocumento {
  id: string;
}

export async function obtenerSala(
  roomId: string,
): Promise<SalaPersistida | null> {
  const snapshot = await obtenerFirestore().collection(ROOMS).doc(roomId).get();
  if (!snapshot.exists) return null;

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
