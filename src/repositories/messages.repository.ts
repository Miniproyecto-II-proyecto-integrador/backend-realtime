import { Timestamp } from "firebase-admin/firestore";
import { obtenerFirestore } from "../config/firebase";
import {
  EliminacionCuentaEnCursoError,
  PerfilEmisorNoEncontradoError,
  SalaNoEncontradaError,
} from "../types/domainErrors";
import { MensajePersistido, NuevoMensaje } from "../types/message";

const ROOMS = "rooms";
const USERS = "users";
const ACCOUNT_DELETIONS = "account-deletions";

export async function guardarMensaje(
  roomId: string,
  mensaje: NuevoMensaje,
): Promise<MensajePersistido> {
  const db = obtenerFirestore();
  const roomRef = db.collection(ROOMS).doc(roomId);
  const deletionRef = db.collection(ACCOUNT_DELETIONS).doc(mensaje.senderUid);
  const senderRef = db.collection(USERS).doc(mensaje.senderUid);
  const messageRef = roomRef.collection("messages").doc();
  const createdAt = Timestamp.now();
  const documento = { ...mensaje, createdAt };

  await db.runTransaction(async (transaction) => {
    const [room, deletion, sender] = await Promise.all([
      transaction.get(roomRef),
      transaction.get(deletionRef),
      transaction.get(senderRef),
    ]);
    if (!room.exists || room.get("closingAt") !== undefined) {
      throw new SalaNoEncontradaError();
    }
    if (deletion.exists) throw new EliminacionCuentaEnCursoError();
    if (!sender.exists) throw new PerfilEmisorNoEncontradoError();
    transaction.create(messageRef, documento);
  });

  return {
    id: messageRef.id,
    roomId,
    ...mensaje,
    createdAt: createdAt.toDate().toISOString(),
  };
}
