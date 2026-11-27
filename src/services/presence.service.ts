import { UsuarioPresente } from "../types/events";

interface UsuarioConexiones {
  usuario: UsuarioPresente;
  membresias: Map<string, symbol>;
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
      conexiones = { usuario, membresias: new Map() };
      usuarios.set(usuario.uid, conexiones);
    }
    if (conexiones.membresias.has(socketId)) return false;
    conexiones.membresias.set(socketId, Symbol());

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
      item.membresias.has(socketId),
    );
    if (!entrada) return null;
    entrada.membresias.delete(socketId);
    this.salasPorSocket.get(socketId)?.delete(roomId);
    if (this.salasPorSocket.get(socketId)?.size === 0) {
      this.salasPorSocket.delete(socketId);
    }

    if (entrada.membresias.size > 0) {
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

  quitarSala(roomId: string): string[] {
    const sockets: string[] = [];
    for (const { membresias } of this.usuariosPorSala.get(roomId)?.values() ?? []) {
      for (const socketId of membresias.keys()) {
        sockets.push(socketId);
        const salas = this.salasPorSocket.get(socketId);
        salas?.delete(roomId);
        if (salas?.size === 0) this.salasPorSocket.delete(socketId);
      }
    }
    this.usuariosPorSala.delete(roomId);
    return sockets;
  }

  tieneUsuarios(roomId: string): boolean {
    return this.usuariosPorSala.has(roomId);
  }

  obtenerMembresia(roomId: string, socketId: string, uid: string): symbol | undefined {
    return this.usuariosPorSala.get(roomId)?.get(uid)?.membresias.get(socketId);
  }

  tieneConexion(roomId: string, socketId: string, uid: string): boolean {
    return this.obtenerMembresia(roomId, socketId, uid) !== undefined;
  }

  usuariosEnSala(roomId: string): UsuarioPresente[] {
    return [...(this.usuariosPorSala.get(roomId)?.values() ?? [])].map(
      ({ usuario }) => usuario,
    );
  }
}
