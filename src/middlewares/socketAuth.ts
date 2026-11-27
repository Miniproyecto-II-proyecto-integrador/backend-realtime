import { Socket } from "socket.io";
import { obtenerAuth } from "../config/firebase";
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
  "auth/invalid-argument",
  "auth/id-token-expired",
  "auth/id-token-revoked",
  "auth/invalid-id-token",
  "auth/user-disabled",
  "auth/user-not-found",
]);

export const verificarTokenFirebase: VerificarToken = async (token) => {
  const { uid, name } = await obtenerAuth().verifyIdToken(token, true);
  return { uid, name };
};

export async function verificarSesionSocket(
  token: unknown,
  verificarToken: VerificarToken,
): Promise<Claims> {
  if (typeof token !== "string" || token.length === 0) {
    throw new Error("AUTH_REQUIRED");
  }

  try {
    return await verificarToken(token);
  } catch (error) {
    const code = codigoError(error);
    // Firebase Admin también usa argument-error si falla la descarga de certificados.
    const falloCertificados = code === "auth/argument-error" && error instanceof Error
      && /^(Error fetching public keys|Error while making request:)/.test(error.message.trim());
    if (!falloCertificados && code && erroresTokenInvalido.has(code)) {
      throw new Error("INVALID_TOKEN");
    }
    console.error(
      `[Socket.IO] no se pudo verificar el ID token: ${code ?? "desconocido"}`,
    );
    throw new Error("AUTH_UNAVAILABLE");
  }
}

export function crearMiddlewareSocketAuth(
  verificarToken: VerificarToken,
): (socket: SocketRealtime, next: (error?: Error) => void) => void {
  return (socket, next) => {
    void verificarSesionSocket(socket.handshake.auth?.token, verificarToken)
      .then((usuario) => {
        socket.data.uid = usuario.uid;
        if (usuario.name) socket.data.displayName = usuario.name;
        next();
      }, next);
  };
}
