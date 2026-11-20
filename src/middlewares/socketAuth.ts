import { Socket } from "socket.io";
import { obtenerAuth } from "../config/firebase";

export async function autenticarSocket(
  socket: Socket,
  next: (error?: Error) => void,
): Promise<void> {
  const token: unknown = socket.handshake.auth?.token;
  if (typeof token !== "string" || token.length === 0) {
    next(new Error("No autorizado"));
    return;
  }

  try {
    const usuario = await obtenerAuth().verifyIdToken(token);
    socket.data.uid = usuario.uid;
    if (usuario.name) socket.data.displayName = usuario.name;
    next();
  } catch (error) {
    const codigo =
      typeof error === "object" && error !== null && "code" in error
        ? String(error.code)
        : "desconocido";
    console.error(`[Socket.IO] no se pudo verificar el ID token: ${codigo}`);
    next(new Error("No autorizado"));
  }
}
