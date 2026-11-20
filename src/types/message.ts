import { Timestamp } from "firebase-admin/firestore";

export interface MensajeDocumento {
  senderUid: string | null;
  senderName: string;
  content: string;
  createdAt: Timestamp;
}

export interface MensajePersistido {
  id: string;
  roomId: string;
  senderUid: string;
  senderName: string;
  content: string;
  createdAt: string;
}

export interface NuevoMensaje {
  senderUid: string;
  senderName: string;
  content: string;
}
