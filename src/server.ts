import { inicializarFirebaseAdmin, obtenerAuth } from "./config/firebase";
import { env } from "./config/env";
import { guardarMensaje } from "./repositories/messages.repository";
import { obtenerSala } from "./repositories/rooms.repository";
import { crearServidorRealtime } from "./app";

inicializarFirebaseAdmin();

const { httpServer } = crearServidorRealtime(
  {
    verifyIdToken: async (token) => {
      const { uid, name } = await obtenerAuth().verifyIdToken(token);
      return { uid, name };
    },
    getRoom: obtenerSala,
    saveMessage: ({ roomId, ...mensaje }) => guardarMensaje(roomId, mensaje),
  },
  env.corsOrigins,
);

httpServer.listen(env.port, () => {
  console.info(`[backend-realtime] escuchando en el puerto ${env.port}`);
});
