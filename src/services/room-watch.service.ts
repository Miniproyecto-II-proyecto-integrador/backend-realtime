import { ObservarSala } from "../types/room";

const REINTENTO_INICIAL_MS = 100;
const REINTENTO_MAXIMO_MS = 5_000;

export interface SalaVigilada {
  cerrada: boolean;
  pendientes: number;
  version: number;
  esperaMs: number;
  cancelar?: () => void;
  reintento?: NodeJS.Timeout;
}

export class VigilanciaSalas {
  private readonly salas = new Map<string, SalaVigilada>();

  constructor(
    private readonly observar: ObservarSala,
    private readonly tieneUsuarios: (roomId: string) => boolean,
    private readonly alCerrar: (roomId: string) => void,
  ) {}

  obtener(roomId: string): SalaVigilada | undefined {
    return this.salas.get(roomId);
  }

  retener(roomId: string): { sala: SalaVigilada; liberar: () => void } {
    let sala = this.salas.get(roomId);
    if (sala) {
      sala.pendientes++;
    } else {
      sala = { cerrada: false, pendientes: 1, version: 0, esperaMs: REINTENTO_INICIAL_MS };
      this.salas.set(roomId, sala);
      this.suscribir(roomId, sala);
    }

    const vigilada = sala;
    let liberada = false;
    return {
      sala: vigilada,
      liberar: () => {
        if (liberada) return;
        liberada = true;
        vigilada.pendientes--;
        this.liberarSiVacia(roomId);
      },
    };
  }

  liberarSiVacia(roomId: string): void {
    const sala = this.salas.get(roomId);
    if (!sala || sala.pendientes > 0 || this.tieneUsuarios(roomId)) return;
    this.salas.delete(roomId);
    this.detener(sala);
  }

  cerrar(roomId: string, sala: SalaVigilada): void {
    if (sala.cerrada || this.salas.get(roomId) !== sala) return;
    sala.cerrada = true;
    this.detener(sala);
    this.alCerrar(roomId);
    this.liberarSiVacia(roomId);
  }

  private detener(sala: SalaVigilada): void {
    sala.version++;
    clearTimeout(sala.reintento);
    sala.reintento = undefined;
    sala.cancelar?.();
    sala.cancelar = undefined;
  }

  private suscribir(roomId: string, sala: SalaVigilada): void {
    const version = ++sala.version;
    const vigente = () => this.salas.get(roomId) === sala && sala.version === version;
    const alFallar = (error: unknown) => {
      if (!vigente()) return;
      console.error("[Firestore] error observando sala:", roomId, error);
      this.detener(sala);
      const esperaMs = sala.esperaMs;
      sala.esperaMs = Math.min(esperaMs * 2, REINTENTO_MAXIMO_MS);
      sala.reintento = setTimeout(() => {
        sala.reintento = undefined;
        this.suscribir(roomId, sala);
      }, esperaMs);
      sala.reintento.unref();
    };

    try {
      const cancelar = this.observar(roomId, (activa) => {
        if (!vigente()) return;
        sala.esperaMs = REINTENTO_INICIAL_MS;
        if (!activa) this.cerrar(roomId, sala);
      }, alFallar);
      if (vigente()) sala.cancelar = cancelar;
      else cancelar();
    } catch (error) {
      alFallar(error);
    }
  }
}
