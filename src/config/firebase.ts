import {
  App,
  cert,
  getApp,
  getApps,
  initializeApp,
} from "firebase-admin/app";
import { Auth, getAuth } from "firebase-admin/auth";
import { Firestore, getFirestore } from "firebase-admin/firestore";
import { env } from "./env";

function obtenerApp(): App {
  if (getApps().length > 0) return getApp();
  if (!env.serviceAccountJson) {
    throw new Error("Falta la variable FIREBASE_SERVICE_ACCOUNT_JSON");
  }

  const credenciales = JSON.parse(env.serviceAccountJson);
  if (typeof credenciales.private_key === "string") {
    credenciales.private_key = credenciales.private_key.replace(/\\n/g, "\n");
  }

  return initializeApp({ credential: cert(credenciales) });
}

export function inicializarFirebaseAdmin(): void {
  obtenerApp();
}

export function obtenerAuth(): Auth {
  return getAuth(obtenerApp());
}

export function obtenerFirestore(): Firestore {
  return getFirestore(obtenerApp());
}
