import { Socket } from "socket.io";
import {
  ClientToServerEvents,
  InterServerEvents,
  ServerToClientEvents,
} from "../types/events";

type SocketRealtime = Socket<
  ClientToServerEvents,
  ServerToClientEvents,
  InterServerEvents
>;
type Claims = { uid: string; name?: string };
type VerificarToken = (token: string) => Promise<Claims>;

function codigoError(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "code" in error
    ? String(error.code)
    : undefined;
}

const erroresTokenInvalido = new Set([
  "auth/argument-error",
  "auth/id-token-expired",
  "auth/id-token-revoked",
  "auth/invalid-id-token",
  "auth/user-disabled",
  "auth/user-not-found",
]);

export function crearMiddlewareSocketAuth(
  verificarToken: VerificarToken,
): (socket: SocketRealtime, next: (error?: Error) => void) => void {
  return (socket, next) => {
    const token: unknown = socket.handshake.auth?.token;
    if (typeof token !== "string" || token.length === 0) {
      next(new Error("AUTH_REQUIRED"));
      return;
    }

    void verificarToken(token)
      .then((usuario) => {
        socket.data.uid = usuario.uid;
        if (usuario.name) socket.data.displayName = usuario.name;
        next();
      })
      .catch((error: unknown) => {
        const code = codigoError(error);
        if (code && erroresTokenInvalido.has(code)) {
          next(new Error("INVALID_TOKEN"));
          return;
        }
        console.error(
          `[Socket.IO] no se pudo verificar el ID token: ${code ?? "desconocido"}`,
        );
        next(new Error("AUTH_UNAVAILABLE"));
      });
  };
}
