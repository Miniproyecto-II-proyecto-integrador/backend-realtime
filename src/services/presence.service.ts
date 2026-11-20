import { UsuarioPresente } from "../types/events";

interface UsuarioConexiones {
  usuario: UsuarioPresente;
  socketIds: Set<string>;
}

interface SalidaPresencia {
  roomId: string;
  usuario: UsuarioPresente;
}

export class PresenceTracker {
  private readonly usuariosPorSala = new Map<
    string,
    Map<string, UsuarioConexiones>
  >();
  private readonly salasPorSocket = new Map<string, Set<string>>();

  agregar(
    roomId: string,
    socketId: string,
    usuario: UsuarioPresente,
  ): boolean {
    let usuarios = this.usuariosPorSala.get(roomId);
    if (!usuarios) {
      usuarios = new Map();
      this.usuariosPorSala.set(roomId, usuarios);
    }

    let conexiones = usuarios.get(usuario.uid);
    const esPrimeraConexion = !conexiones;
    if (!conexiones) {
      conexiones = { usuario, socketIds: new Set() };
      usuarios.set(usuario.uid, conexiones);
    }
    if (conexiones.socketIds.has(socketId)) return false;
    conexiones.socketIds.add(socketId);

    let salas = this.salasPorSocket.get(socketId);
    if (!salas) {
      salas = new Set();
      this.salasPorSocket.set(socketId, salas);
    }
    salas.add(roomId);
    return esPrimeraConexion;
  }

  quitar(
    roomId: string,
    socketId: string,
  ): { usuario: UsuarioPresente; quedoOffline: boolean } | null {
    const usuarios = this.usuariosPorSala.get(roomId);
    if (!usuarios) return null;

    const entrada = [...usuarios.values()].find((item) =>
      item.socketIds.has(socketId),
    );
    if (!entrada) return null;
    entrada.socketIds.delete(socketId);
    this.salasPorSocket.get(socketId)?.delete(roomId);
    if (this.salasPorSocket.get(socketId)?.size === 0) {
      this.salasPorSocket.delete(socketId);
    }

    if (entrada.socketIds.size > 0) {
      return { usuario: entrada.usuario, quedoOffline: false };
    }
    usuarios.delete(entrada.usuario.uid);
    if (usuarios.size === 0) this.usuariosPorSala.delete(roomId);
    return { usuario: entrada.usuario, quedoOffline: true };
  }

  quitarSocket(socketId: string): SalidaPresencia[] {
    const salas = [...(this.salasPorSocket.get(socketId) ?? [])];
    const salidas: SalidaPresencia[] = [];
    for (const roomId of salas) {
      const resultado = this.quitar(roomId, socketId);
      if (resultado?.quedoOffline) {
        salidas.push({ roomId, usuario: resultado.usuario });
      }
    }
    this.salasPorSocket.delete(socketId);
    return salidas;
  }

  tieneConexion(roomId: string, socketId: string, uid: string): boolean {
    return (
      this.usuariosPorSala
        .get(roomId)
        ?.get(uid)
        ?.socketIds.has(socketId) ?? false
    );
  }

  usuariosEnSala(roomId: string): UsuarioPresente[] {
    return [...(this.usuariosPorSala.get(roomId)?.values() ?? [])].map(
      ({ usuario }) => usuario,
    );
  }
}
