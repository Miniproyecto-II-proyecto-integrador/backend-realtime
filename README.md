# backend-realtime

Servicio independiente de Node.js, TypeScript y Socket.IO. Comparte el proyecto
Firebase y la base de Firestore con `backend-api`; no crea una colección
alternativa de salas ni modifica el esquema de US-06.

## Modelo de Firestore

`backend-api` es el dueño de la creación de salas. Los documentos existentes
permanecen en `rooms/{roomId}` con el esquema:

```text
rooms/{roomId}
  nombre: string
  ownerUid: string
  createdAt: Timestamp
```

El ID de la sala es el ID del documento. Los mensajes se modelan como
subcolección de la sala, no como una colección global:

```text
rooms/{roomId}/messages/{messageId}
  senderUid: string
  senderName: string
  content: string
  createdAt: Timestamp
```

`senderName` es una instantánea del nombre mostrado al enviar el mensaje. La
política de eliminación de cuenta requiere conservar los mensajes de salas
ajenas y anonimizar su autor; cuando se implemente persistencia de mensajes,
esa limpieza deberá anonimizar tanto `senderName` como `senderUid`. Esta fase
solo define y documenta el modelo: todavía no persiste, distribuye ni consulta
mensajes.

No se amplía el documento de sala con participantes o presencia en esta fase.
La presencia es temporal y se podrá modelar después; los miembros persistentes
deberán definirse cuando se implemente la autorización para colaboración.

## Estructura

- `src/server.ts`: servidor HTTP, endpoint de salud y montaje de Socket.IO.
- `src/config`: configuración de entorno y Firebase Admin.
- `src/middlewares/socketAuth.ts`: verifica el ID token Firebase al conectar.
- `src/sockets`: configuración de Socket.IO; no contiene eventos de chat aún.
- `src/repositories/rooms.repository.ts`: lectura del documento de sala
  existente en `rooms`.
- `src/types`: tipos de sala, mensaje y datos autenticados del socket.

## Variables de entorno

Copiar `.env.example` como `.env` y configurar:

- `PORT`: puerto HTTP/Socket.IO (por defecto `3001`).
- `CORS_ORIGINS`: orígenes frontend separados por coma.
- `FIREBASE_SERVICE_ACCOUNT_JSON`: JSON completo de la cuenta de servicio
  Firebase. La cuenta debe apuntar al mismo proyecto configurado en
  `backend-api`.

No guardar credenciales reales en el repositorio. Cada servicio es un proceso
independiente y debe inicializar Firebase Admin en su propio proceso; ambos usan
las mismas credenciales/proyecto y las mismas rutas de Firestore.

## Desarrollo

```bash
npm install
cp .env.example .env
# Completar FIREBASE_SERVICE_ACCOUNT_JSON en .env
npm run dev
```

El endpoint `GET /health` comprueba que el servicio está activo. La conexión
Socket.IO requiere un ID token Firebase en el campo `auth.token` del handshake.
Todavía no existen eventos para unirse a salas ni enviar mensajes.

## Build

```bash
npm run build
npm start
```
