export interface UsuarioPresente {
  uid: string;
  displayName: string;
}

export interface SalaUnidaPayload {
  roomId: string;
  roomName: string;
  onlineUsers: UsuarioPresente[];
}

export interface SalaErrorPayload {
  roomId?: string;
  code: string;
  message: string;
}

export interface MensajeNuevoPayload {
  id: string;
  roomId: string;
  senderUid: string;
  senderName: string;
  content: string;
  createdAt: string;
}

export interface PresenceUpdatePayload {
  roomId: string;
  uid: string;
  displayName: string;
  status: "online" | "offline";
}

export interface ClientToServerEvents {
  "room:join": (payload: { roomId: string }) => void;
  "room:leave": (payload: { roomId: string }) => void;
  "message:send": (payload: { roomId: string; content: string }) => void;
}

export interface ServerToClientEvents {
  "room:joined": (payload: SalaUnidaPayload) => void;
  "room:left": (payload: { roomId: string }) => void;
  "room:error": (payload: SalaErrorPayload) => void;
  "message:new": (payload: MensajeNuevoPayload) => void;
  "presence:update": (payload: PresenceUpdatePayload) => void;
}

export interface InterServerEvents {}
