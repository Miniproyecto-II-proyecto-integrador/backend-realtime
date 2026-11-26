import { Timestamp } from "firebase-admin/firestore";

export interface SalaDocumento {
  nombre: string;
  ownerUid: string;
  createdAt: Timestamp;
  closingAt?: Timestamp;
}

export type ObservarSala = (
  roomId: string,
  alCambiar: (activa: boolean) => void,
  alFallar: (error: unknown) => void,
) => () => void;
