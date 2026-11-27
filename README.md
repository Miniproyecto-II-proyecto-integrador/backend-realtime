# backend-realtime

Servicio independiente Node.js + TypeScript + Socket.IO. Comparte el mismo
proyecto Firebase/Firestore que `backend-api`; no crea una colección alternativa
de salas ni cambia los documentos existentes de US-06.

## Modelo de Firestore

Las salas existentes viven en `rooms/{roomId}`:

```text
rooms/{roomId}
  nombre: string
  ownerUid: string
  createdAt: Timestamp
```

Los mensajes se guardan en la subcolección de cada sala:

```text
rooms/{roomId}/messages/{messageId}
  senderUid: string | null
  senderName: string
  content: string
  createdAt: Timestamp
```

En un mensaje nuevo `senderUid` contiene el UID verificado del socket. Al
eliminar una cuenta, `backend-api` conserva los mensajes de salas ajenas, pero
establece `senderUid` en `null` y `senderName` en `Usuario eliminado`. Las salas
propias y sus descendientes se eliminan recursivamente. Las escrituras de chat
comprueban el marcador de eliminación de cuenta en la misma transacción que
crea el mensaje.

## Acceso a salas y presencia

La política aprobada para esta fase es que el ID de sala funciona como una
invitación: cualquier usuario autenticado que conozca el ID puede entrar. Se
consulta `rooms/{roomId}` antes de aceptar el ingreso. El esquema actual no
guarda participantes ni roles.

La presencia se mantiene en memoria por instancia y por sala, contando sockets
activos por UID. Un usuario con varias conexiones permanece online hasta que
sale su último socket de esa sala. Al desconectarse un socket, sus membresías se
limpian automáticamente.

Esta estrategia funciona con una sola instancia. Si Render ejecuta varias
instancias, cada una tendría su propia presencia y sus propias salas Socket.IO;
los eventos no cruzarían instancias. Para escalar horizontalmente se requiere
un adaptador compartido (por ejemplo, Socket.IO Redis Adapter) y una estrategia
compartida de presencia. Firestore no se usa como almacén de presencia en esta
fase.

## Eventos Socket.IO

La autenticación se envía al conectar:

```ts
io(url, { auth: { token: firebaseIdToken } });
```

| Evento | Dirección | Payload / respuesta | Errores |
|---|---|---|---|
| `connection` | cliente → servidor (handshake; aceptación del servidor) | Conexión aceptada tras verificar el token. | `connect_error`: `AUTH_REQUIRED`, `INVALID_TOKEN` o `AUTH_UNAVAILABLE`. |
| `room:join` | cliente → servidor | `{ roomId }`. Los ingresos simultáneos para el mismo socket y sala comparten un solo resultado. | `room:error`: `INVALID_ROOM_ID`, `ROOM_NOT_FOUND`, `ROOM_LOOKUP_FAILED`, `ROOM_JOIN_FAILED`, `ROOM_JOIN_TIMEOUT` o `TOO_MANY_PENDING_JOINS`. |
| `room:joined` | servidor → cliente | `{ roomId, roomName, onlineUsers: [{ uid, displayName }] }`. | Se emite solo después de comprobar y unir el socket a la sala. |
| `room:leave` | cliente → servidor | `{ roomId }`. | `room:error`: `INVALID_ROOM_ID`, `NOT_IN_ROOM` o `ROOM_LEAVE_FAILED`. |
| `room:left` | servidor → cliente | `{ roomId }` tras completar una salida solicitada, incluso si coincide con el cierre. | No se emite si la solicitud de salida es inválida. |
| `room:deleted` | servidor → miembros conectados de la sala | `{ roomId }`. Se emite al observar el cierre o la eliminación, también si se originó al eliminar una cuenta. | Los sockets salen solo de esa sala y se elimina su presencia en ella. |
| `room:error` | servidor → cliente | `{ roomId?, code, message }`. | Respuesta de error a operaciones de sala o mensaje. |
| `message:send` | cliente → servidor | `{ roomId, content }`; máximo 2000 caracteres. El emisor se obtiene de la sesión y necesita un perfil existente. | `room:error`: `INVALID_ROOM_ID`, `NOT_IN_ROOM`, `INVALID_CONTENT`, `ROOM_NOT_FOUND`, `ACCOUNT_DELETION_IN_PROGRESS`, `ACCOUNT_NOT_AVAILABLE` o `MESSAGE_PERSISTENCE_FAILED`. |
| `message:new` | servidor → miembros de la sala | `{ id, roomId, senderUid, senderName, content, createdAt }`. Se emite después de guardar en Firestore. | Si falla el guardado, no se emite. |
| `presence:update` | servidor → otros miembros de la sala | `{ roomId, uid, displayName, status: "online" \| "offline" }`. Solo cambia al conectarse el primer socket o desconectarse el último del usuario en esa sala. | No se emite por una desconexión individual si quedan otros sockets activos. |
| `disconnect` | cliente o servidor → contraparte | Evento Socket.IO sin payload; el servidor limpia la presencia de ese socket. | No aplica. |

El servidor obtiene la identidad exclusivamente de `verifyIdToken` durante el
handshake. No acepta UID del payload para unirse o enviar mensajes.

El documento `rooms/{roomId}` admite `closingAt: Timestamp` opcional. Su presencia
bloquea ingresos y escrituras mientras la API limpia las subcolecciones. El ingreso
a una sala en cierre o eliminada devuelve `room:error` con `code: "ROOM_NOT_FOUND"`.
El cliente debe salir de la vista de sala al recibir ese error o `room:deleted`.
Si se pierde el evento durante una desconexión, el siguiente `room:join` vuelve
a verificar la sala.

Realtime observa un documento por sala con miembros locales, ingresos o escrituras
pendientes. Libera la suscripción cuando termina el último uso. Ante un error
terminal vuelve a suscribirse con espera incremental mientras la sala siga en uso.
Las comprobaciones contra Firestore continúan durante ese intervalo. Cada socket
admite hasta ocho ingresos pendientes y cada ingreso tiene un plazo de cinco segundos.

## Variables de entorno

Copiar `.env.example` a `.env` para desarrollo local:

- `PORT`: puerto HTTP/Socket.IO (por defecto `3001`; Render asigna `PORT`).
- `CORS_ORIGINS`: orígenes frontend separados por coma.
- `FIREBASE_SERVICE_ACCOUNT_JSON`: JSON completo de la cuenta de servicio del
  mismo proyecto que usa `backend-api`.

No agregar `.env` ni credenciales al repositorio o a la imagen Docker.

## Desarrollo y pruebas

Usa Node.js 22 para reproducir la versión de la imagen Docker en local.

```bash
npm install
cp .env.example .env
# Completar FIREBASE_SERVICE_ACCOUNT_JSON localmente
npm run dev
npm test
npm run build
```

Las pruebas usan dependencias simuladas y no escriben en Firestore real.
`GET /health` devuelve `{"estado":"ok"}`.

## Docker y Render

El Dockerfile usa Node.js 22 en las etapas de compilación y ejecución. La imagen
final instala las dependencias de producción y arranca como usuario `node`.

**US-07 requiere desplegar realtime antes de la API.** Confirma el commit nuevo en
Render y la terminación de todas las instancias antiguas, incluidas las que
todavía mantienen WebSockets. Consulta el procedimiento de verificación y rollback
en la sección de despliegue del README de `backend-api`.

Construir y ejecutar localmente:

```bash
docker build -t backend-realtime .
docker run --rm -p 3001:3001 \
  -e PORT=3001 \
  -e CORS_ORIGINS=http://localhost:5173 \
  -e FIREBASE_SERVICE_ACCOUNT_JSON \
  backend-realtime
```

En Render, crea un **Web Service** con runtime **Docker** conectado al
repositorio independiente `backend-realtime`. Deja **Root Directory** en su
valor predeterminado y usa `./Dockerfile` como **Dockerfile Path**. Configura
`FIREBASE_SERVICE_ACCOUNT_JSON` y `CORS_ORIGINS` en el panel de variables de
entorno. Render proporciona `PORT`. Usa `/health` como health check y mantén
una sola instancia mientras la presencia y los eventos sean locales.
