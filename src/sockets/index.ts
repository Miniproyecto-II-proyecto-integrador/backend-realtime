import { Server } from "socket.io";
import { env } from "../config/env";
import { autenticarSocket } from "../middlewares/socketAuth";

export function configurarSockets(io: Server): void {
  io.use(autenticarSocket);
}

export function crearOpcionesSocket() {
  return {
    cors: {
      origin: env.corsOrigins,
      methods: ["GET", "POST"],
    },
  };
}
