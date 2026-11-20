import { Timestamp } from "firebase-admin/firestore";
import { obtenerFirestore } from "../config/firebase";
import {
  EliminacionCuentaEnCursoError,
  SalaNoEncontradaError,
} from "../types/domainErrors";
import { MensajePersistido, NuevoMensaje } from "../types/message";

const ROOMS = "rooms";
const ACCOUNT_DELETIONS = "account-deletions";

export async function guardarMensaje(
  roomId: string,
  mensaje: NuevoMensaje,
): Promise<MensajePersistido> {
  const db = obtenerFirestore();
  const roomRef = db.collection(ROOMS).doc(roomId);
  const deletionRef = db.collection(ACCOUNT_DELETIONS).doc(mensaje.senderUid);
  const messageRef = roomRef.collection("messages").doc();
  const createdAt = Timestamp.now();
  const documento = { ...mensaje, createdAt };

  await db.runTransaction(async (transaction) => {
    const [room, deletion] = await Promise.all([
      transaction.get(roomRef),
      transaction.get(deletionRef),
    ]);
    if (!room.exists) throw new SalaNoEncontradaError();
    if (deletion.exists) throw new EliminacionCuentaEnCursoError();
    transaction.create(messageRef, documento);
  });

  return {
    id: messageRef.id,
    roomId,
    ...mensaje,
    createdAt: createdAt.toDate().toISOString(),
  };
}
