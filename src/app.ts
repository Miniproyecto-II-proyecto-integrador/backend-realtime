import { createServer, Server as HttpServer } from "node:http";
import { Server } from "socket.io";
import { PresenceTracker } from "./services/presence.service";
import { configurarSockets, RealtimeDependencies } from "./sockets";
import {
  ClientToServerEvents,
  InterServerEvents,
  ServerToClientEvents,
} from "./types/events";

export type RealtimeSocketServer = Server<
  ClientToServerEvents,
  ServerToClientEvents,
  InterServerEvents
>;

export function crearServidorRealtime(
  dependencies: RealtimeDependencies,
  corsOrigins: string[],
): {
  httpServer: HttpServer;
  io: RealtimeSocketServer;
  presence: PresenceTracker;
} {
  const httpServer = createServer((request, response) => {
    if (request.method === "GET" && request.url === "/health") {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ estado: "ok" }));
      return;
    }

    response.writeHead(404, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ error: "Ruta no encontrada" }));
  });
  const io = new Server<
    ClientToServerEvents,
    ServerToClientEvents,
    InterServerEvents
  >(httpServer, {
    cors: {
      origin: corsOrigins,
      methods: ["GET", "POST"],
    },
  });
  const presence = new PresenceTracker();
  configurarSockets(io, dependencies, presence);

  return { httpServer, io, presence };
}
