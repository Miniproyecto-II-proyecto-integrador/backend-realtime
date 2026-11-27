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
| `message:send` | cliente → servidor | `{ roomId, content }`; texto recortado de 1 a 2000 caracteres. El servidor verifica la sesión y la membresía. Admite hasta ocho envíos en curso por socket. | `room:error`: `INVALID_ROOM_ID`, `NOT_IN_ROOM`, `INVALID_CONTENT`, `AUTH_REQUIRED`, `INVALID_TOKEN`, `AUTH_UNAVAILABLE`, `TOO_MANY_PENDING_MESSAGES`, `ROOM_NOT_FOUND`, `ACCOUNT_DELETION_IN_PROGRESS`, `ACCOUNT_NOT_AVAILABLE` o `MESSAGE_PERSISTENCE_FAILED`. |
| `message:new` | servidor → miembros de la sala | `{ id, roomId, senderUid, senderName, content, createdAt }`. Se emite después de guardar en Firestore. | Si falla el guardado, no se emite. |
| `presence:update` | servidor → otros miembros de la sala | `{ roomId, uid, displayName, status: "online" \| "offline" }`. Solo cambia al conectarse el primer socket o desconectarse el último del usuario en esa sala. | No se emite por una desconexión individual si quedan otros sockets activos. |
| `disconnect` | Socket.IO → cliente y servidor | Evento local de Socket.IO con el motivo de desconexión. El servidor limpia la presencia de ese socket. | No aplica. |

El servidor obtiene la identidad del token verificado durante el handshake.
Firebase Auth comprueba también la revocación y el estado de la cuenta al conectar,
al ingresar y al enviar un mensaje. Un socket que siga abierto después de que su
token venza o se revoque, o su cuenta se elimine o deshabilite, recibe
`room:error` con `code: "INVALID_TOKEN"` al intentar ingresar o enviar. La comprobación
ocurre antes de consultar o escribir en Firestore. En `room:join` comparte el plazo
de cinco segundos del ingreso.

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
Si el fallo se produjo al enviar, conserva el borrador para un reintento explícito.

### Chat tras una desconexión

Realtime recibe `message:send`, vuelve a comprobar la sesión y confirma que la
membresía original del socket sigue vigente. Si el socket salió y volvió a
ingresar mientras se verificaba la sesión, el envío anterior se descarta.
Firestore crea el mensaje en una transacción que comprueba que la sala siga
activa y que el emisor tenga perfil y no esté eliminando su cuenta. Si la escritura
ya comenzó con autorización válida, puede completarse después de que salga el
emisor. Tras guardar se emite `message:new` a quienes sigan en la sala. La emisión
se omite si ya se observó su cierre. Si falla la escritura, el cliente recibe un
error cuando su conexión sigue activa y el servidor todavía está atendiendo esa sala.

El frontend obtiene el token y dirige la reconexión. Cada conexión nueva requiere
un nuevo `room:join`. Instala un solo listener de `message:new` por cliente antes
de reingresar, filtra por sala y acumula los eventos mientras recuperas el historial.
Usa el último mensaje conocido de un tramo continuo como punto de referencia.
Captúralo antes de mezclar los eventos nuevos.

```text
al desconectar:
  pausar envíos y cancelar la recuperación anterior
  guardar el ID del último mensaje del tramo continuo
  reconectar con espera progresiva y un token vigente

al conectar:
  empezar un búfer de eventos para la sala activa
  emitir room:join({ roomId }) y esperar room:joined
  pedir GET /api/rooms/{roomId}/messages con Authorization: Bearer <token>
  mientras exista un ID de referencia, todavía falte y hasMore sea true:
    pedir la página anterior usando nextCursor de esta misma sala
  si se encontró el ID de referencia y hasMore es true:
    opcionalmente pedir una página anterior adicional como solapamiento
  combinar páginas, caché y búfer por (roomId, id)
  aplicar los eventos siguientes a la misma colección y habilitar los envíos
```

`backend-api` (US-11) devuelve `{ messages, nextCursor, hasMore }`. Conserva el
orden cronológico de cada página, antepone las páginas anteriores según su cursor
y trata `nextCursor` como opaco. El historial puede incluir `senderUid: null`
por anonimización. Usa la copia del historial si un mismo ID aparece en ambas
fuentes, así se reflejan los datos anonimizados. Si el cursor dejó de ser válido,
repite la recuperación desde la página más reciente.

Cada página del historial se lee en una instantánea independiente. Encontrar el
ID de referencia puede dejar fuera escrituras tardías, porque un mensaje puede
confirmarse después con un `createdAt` anterior a esa referencia o quedar en un
tramo que ya se consultó. Volver a leer un tramo acotado de páginas alrededor de
la referencia reduce el riesgo de omisiones. Aun con ese solapamiento pueden
quedar escrituras fuera del tramo rescatado.

Si llega `room:deleted`, `room:error` con `ROOM_NOT_FOUND` al reingresar, o un
`404` del historial para esa sala, cancela la recuperación y sus reintentos y sal
de la sala en la interfaz. Al cambiar de sala o desconectar de nuevo, descarta
las respuestas y el búfer del intento anterior.

Deduplicar por `(roomId, id)` evita mostrar dos veces el mismo documento cuando
llega por el historial y por Socket.IO. Si la conexión se corta antes de recibir
el evento de un envío, el historial puede ayudar a encontrarlo y el resultado
aún puede quedar indeterminado para el cliente. El sistema carece de claves de
idempotencia y de confirmaciones por envío. La entrega exactamente una vez queda
fuera de las garantías del contrato. Un reenvío de resultado incierto puede crear
un documento duplicado con otro ID. Recupera el historial antes de ofrecer un
reenvío manual.

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
