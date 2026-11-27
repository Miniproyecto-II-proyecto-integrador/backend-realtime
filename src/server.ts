import { inicializarFirebaseAdmin } from "./config/firebase";
import { env } from "./config/env";
import { verificarTokenFirebase } from "./middlewares/socketAuth";
import { guardarMensaje } from "./repositories/messages.repository";
import { observarSala, obtenerSala } from "./repositories/rooms.repository";
import { crearServidorRealtime } from "./app";

inicializarFirebaseAdmin();

const { httpServer } = crearServidorRealtime(
  {
    verifyIdToken: verificarTokenFirebase,
    getRoom: obtenerSala,
    watchRoom: observarSala,
    saveMessage: ({ roomId, ...mensaje }) => guardarMensaje(roomId, mensaje),
  },
  env.corsOrigins,
);

httpServer.listen(env.port, () => {
  console.info(`[backend-realtime] escuchando en el puerto ${env.port}`);
});
