export class SalaNoEncontradaError extends Error {
  constructor() {
    super("La sala ya no existe");
    this.name = "SalaNoEncontradaError";
  }
}

export class EliminacionCuentaEnCursoError extends Error {
  constructor() {
    super("La cuenta está en proceso de eliminación");
    this.name = "EliminacionCuentaEnCursoError";
  }
}
