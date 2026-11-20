import "socket.io";

declare module "socket.io" {
  interface SocketData {
    uid: string;
    displayName?: string;
  }
}
