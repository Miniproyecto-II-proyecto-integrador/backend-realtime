import { Timestamp } from "firebase-admin/firestore";

export interface SalaDocumento {
  nombre: string;
  ownerUid: string;
  createdAt: Timestamp;
}
