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
| `connect` | Socket.IO → cliente | Confirma la conexión tras verificar el token del handshake. | `connect_error.message`: `AUTH_REQUIRED`, `INVALID_TOKEN` o `AUTH_UNAVAILABLE`. |
| `room:join` | cliente → servidor | `{ roomId }`. Los ingresos simultáneos para el mismo socket y sala comparten un resultado mientras el intento está vigente. | `room:error`: `INVALID_ROOM_ID`, `INVALID_TOKEN`, `AUTH_REQUIRED`, `AUTH_UNAVAILABLE`, `ROOM_NOT_FOUND`, `ROOM_LOOKUP_FAILED`, `ROOM_JOIN_FAILED`, `ROOM_JOIN_TIMEOUT` o `TOO_MANY_PENDING_JOINS`. |
| `room:joined` | servidor → cliente | `{ roomId, roomName, onlineUsers: [{ uid, displayName }] }`. | Se emite solo después de comprobar y unir el socket a la sala. |
| `room:leave` | cliente → servidor | `{ roomId }`. | `room:error`: `INVALID_ROOM_ID`, `NOT_IN_ROOM` o `ROOM_LEAVE_FAILED`. |
| `room:left` | servidor → cliente | `{ roomId }` tras completar una salida solicitada, incluso si coincide con el cierre. | No se emite si la solicitud de salida es inválida. |
| `room:deleted` | servidor → miembros conectados de la sala | `{ roomId }`. Se emite al observar el cierre o la eliminación, también si se originó al eliminar una cuenta. | Los sockets salen solo de esa sala y se elimina su presencia en ella. |
| `room:error` | servidor → cliente | `{ roomId?, code, message }`. | Respuesta de error a operaciones de sala o mensaje. |
| `message:send` | cliente → servidor | `{ roomId, content }`; máximo 2000 caracteres. El emisor se obtiene de la sesión y necesita un perfil existente. | `room:error`: `INVALID_ROOM_ID`, `NOT_IN_ROOM`, `INVALID_CONTENT`, `ROOM_NOT_FOUND`, `ACCOUNT_DELETION_IN_PROGRESS`, `ACCOUNT_NOT_AVAILABLE` o `MESSAGE_PERSISTENCE_FAILED`. |
| `message:new` | servidor → miembros de la sala | `{ id, roomId, senderUid, senderName, content, createdAt }`. Se emite después de guardar en Firestore. | Si falla el guardado, no se emite. |
| `presence:update` | servidor → otros miembros de la sala | `{ roomId, uid, displayName, status: "online" \| "offline" }`. Solo cambia al conectarse el primer socket o desconectarse el último del usuario en esa sala. | No se emite por una desconexión individual si quedan otros sockets activos. |
| `disconnect` | Socket.IO → cliente y servidor | Evento local de Socket.IO con el motivo de desconexión. El servidor limpia la presencia de ese socket. | No aplica. |

El servidor obtiene la identidad del token verificado durante el handshake.
Firebase Auth comprueba también la revocación y el estado de la cuenta al conectar
y antes de admitir cada `room:join`. Un socket que siga abierto después de que su
token venza o se revoque, o su cuenta se elimine o deshabilite, recibe
`room:error` con `code: "INVALID_TOKEN"` al intentar un ingreso. La comprobación
ocurre antes de consultar Firestore y comparte el plazo de cinco segundos del ingreso.

Para `room:join`, `roomId` debe ser texto. Se recorta y debe tener entre 1 y 128
caracteres. Se rechazan barras, caracteres de control, `.` y `..`, e IDs reservados
del tipo `__...__`. La API y Socket.IO usan el mismo ID público en sus contratos.
Los canales internos de estudio se distinguen del canal privado de cada socket.

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
Si el plazo vence mientras Firebase Auth sigue verificando, el cliente recibe
`ROOM_JOIN_TIMEOUT` y esa verificación sigue ocupando un cupo hasta que termina.
El servidor responde `TOO_MANY_PENDING_JOINS` si se agotan los cupos o si se
reintenta la misma sala mientras el intento vencido todavía ocupa su cupo.

### Renovación del token

El mismo flujo aplica a `INVALID_TOKEN` en `room:error.code` y `connect_error.message`.

1. Obtén un ID token vigente con `firebaseUser.getIdToken(true)`. Si Firebase
   exige iniciar sesión de nuevo, completa ese paso antes de continuar.
2. Cierra la conexión anterior si sigue abierta y asigna `socket.auth = { token }`.
3. Registra un listener con `socket.once("connect", ...)` antes de llamar a
   `socket.connect()` para establecer una nueva conexión autenticada.
4. Al recibir `connect`, vuelve a emitir `room:join` con el `roomId` original.

Ante `AUTH_UNAVAILABLE`, conserva el token y aplica una espera progresiva entre
reintentos. Si falló el handshake, reintenta la conexión. Si el socket sigue
conectado y falló el ingreso, vuelve a emitir `room:join` después de la espera.

## Variables de entorno

Copiar `.env.example` a `.env` para desarrollo local:

- `PORT`: puerto HTTP/Socket.IO (por defecto `3001`; Render asigna `PORT`).
- `CORS_ORIGINS`: orígenes frontend separados por coma.
- `FIREBASE_SERVICE_ACCOUNT_JSON`: JSON completo de la cuenta de servicio del
  mismo proyecto que usa `backend-api`.

No agregar `.env` ni credenciales al repositorio o a la imagen Docker.

## Desarrollo y pruebas

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

En Render, crear un servicio de tipo **Web Service** con runtime **Docker** y
configurar la raíz del servicio como `backend-realtime` (o indicar
`backend-realtime/Dockerfile` si la raíz es el repositorio). Configurar
`FIREBASE_SERVICE_ACCOUNT_JSON` y `CORS_ORIGINS` en el panel de variables
secretas; Render proporciona `PORT`. Usar `/health` como health check.
Mantener una instancia mientras no se configure un adaptador distribuido para
Socket.IO.
