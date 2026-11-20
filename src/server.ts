import { createServer } from "node:http";
import { Server } from "socket.io";
import { inicializarFirebaseAdmin } from "./config/firebase";
import { env } from "./config/env";
import { crearOpcionesSocket, configurarSockets } from "./sockets";

const httpServer = createServer((request, response) => {
  if (request.method === "GET" && request.url === "/health") {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ estado: "ok" }));
    return;
  }

  response.writeHead(404, { "Content-Type": "application/json" });
  response.end(JSON.stringify({ error: "Ruta no encontrada" }));
});

inicializarFirebaseAdmin();

const io = new Server(httpServer, crearOpcionesSocket());
configurarSockets(io);

httpServer.listen(env.port, () => {
  console.info(`[backend-realtime] escuchando en el puerto ${env.port}`);
});
