import "dotenv/config";

function leerPuerto(valor: string | undefined): number {
  const puerto = Number(valor ?? 3001);
  if (!Number.isInteger(puerto) || puerto < 1 || puerto > 65535) {
    throw new Error("PORT debe ser un entero entre 1 y 65535");
  }
  return puerto;
}

export const env = {
  port: leerPuerto(process.env.PORT),
  corsOrigins: (process.env.CORS_ORIGINS ?? "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean),
  serviceAccountJson: process.env.FIREBASE_SERVICE_ACCOUNT_JSON ?? "",
};
