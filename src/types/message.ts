import { Timestamp } from "firebase-admin/firestore";

export interface MensajeDocumento {
  senderUid: string;
  senderName: string;
  content: string;
  createdAt: Timestamp;
}
