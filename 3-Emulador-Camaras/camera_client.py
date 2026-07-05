# -*- coding: utf-8 -*-
"""
DIA 3 - Emulador de Camaras IP
================================
Simula 3 camaras IP que envian imagenes (como vectores de caracteristicas)
al cluster Raft en Java.

Protocolo: CLI_REQ|CAMARA_N|0|color,aspecto,textura\n
           (igual que el protocolo Tipo|Sender|Term|Payload\n del cluster)

Cada camara corre en su propio hilo y envia 3 "fotos" con 3s de pausa.

Uso:
    python camera_client.py          # modo interactivo (pide ENTER)
    python camera_client.py --auto   # modo automatico (para pruebas)
"""

import sys
import os
import socket
import threading
import time

# ─── Fix de codificacion en Windows ───────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURACION
# ─────────────────────────────────────────────────────────────────────
CLUSTER_HOST  = "127.0.0.1"
CLUSTER_PORTS = [8001, 8002, 8003]   # Intentara conectar al lider
NUM_FOTOS     = 3                    # Fotos por camara
PAUSA_FOTOS   = 3                    # Segundos entre fotos

# Datasets simulados: cada camara captura un tipo de objeto distinto
DATASETS = {
    "CAMARA_1": [  # Normalmente ve PERROS
        [118, 1.22, 47],
        [125, 1.18, 43],
        [122, 1.25, 48],
    ],
    "CAMARA_2": [  # Normalmente ve GATOS
        [88,  0.92, 72],
        [93,  0.87, 68],
        [91,  0.95, 71],
    ],
    "CAMARA_3": [  # Normalmente ve CARROS
        [205, 2.48, 12],
        [198, 2.52, 18],
        [202, 2.45, 14],
    ],
}

# ─────────────────────────────────────────────────────────────────────
#  FUNCION PRINCIPAL DE ENVIO
# ─────────────────────────────────────────────────────────────────────

def enviar_y_recibir(camara_id, foto_num, vector):
    """
    Abre una conexion al cluster, envia el vector como CLI_REQ y espera CLI_RES.
    Si recibe REDIRECT, reintenta con otro nodo automaticamente.
    """
    payload = f"{vector[0]},{vector[1]},{vector[2]}"
    trama   = f"CLI_REQ|{camara_id}|0|{payload}\n"

    # Probar todos los puertos hasta encontrar al lider
    puertos_intentados = set()
    puertos_a_probar   = list(CLUSTER_PORTS)

    while puertos_a_probar:
        puerto = puertos_a_probar.pop(0)
        if puerto in puertos_intentados:
            continue
        puertos_intentados.add(puerto)

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((CLUSTER_HOST, puerto))
            print(f"[{camara_id}] Foto {foto_num} -> enviando a puerto {puerto}: {payload}")
            sock.sendall(trama.encode("utf-8"))

            # Esperar respuesta completa (hasta \n) — 20s para dar tiempo al WorkerPool
            sock.settimeout(20)
            respuesta_raw = b""
            while b"\n" not in respuesta_raw:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                respuesta_raw += chunk

            sock.close()
            respuesta = respuesta_raw.decode("utf-8", errors="replace").strip()

            # Si el nodo no es lider, reintenta con otro
            if respuesta.startswith("REDIRECT"):
                print(f"[{camara_id}] Redirigido por puerto {puerto}. Buscando lider...")
                for p in CLUSTER_PORTS:
                    if p not in puertos_intentados:
                        puertos_a_probar.insert(0, p)
                continue

            # Exito: CLI_RES recibido
            print(f"[{camara_id}] OK Respuesta: {respuesta}")
            return

        except socket.timeout:
            print(f"[{camara_id}] Timeout en puerto {puerto} para foto {foto_num}.")
        except ConnectionRefusedError:
            print(f"[{camara_id}] Nodo en puerto {puerto} no disponible.")
        except Exception as e:
            print(f"[{camara_id}] Error en puerto {puerto}: {e}")

    print(f"[{camara_id}] ERROR: Ningun nodo respondio CLI_RES para foto {foto_num}.")


# ─────────────────────────────────────────────────────────────────────
#  HILO DE CAMARA
# ─────────────────────────────────────────────────────────────────────

def hilo_camara(camara_id, fotos):
    """
    Funcion que corre en su propio Thread.
    Envia NUM_FOTOS imagenes con pausa entre ellas.
    """
    print(f"\n{'='*50}")
    print(f"  [{camara_id}] Camara iniciada. Enviara {NUM_FOTOS} fotos.")
    print(f"{'='*50}")

    for i, foto in enumerate(fotos[:NUM_FOTOS], start=1):
        enviar_y_recibir(camara_id, i, foto)
        if i < NUM_FOTOS:
            print(f"[{camara_id}] Esperando {PAUSA_FOTOS}s antes de la siguiente foto...")
            time.sleep(PAUSA_FOTOS)

    print(f"[{camara_id}] Camara termino de enviar todas sus fotos.")


# ─────────────────────────────────────────────────────────────────────
#  MAIN — Lanza las 3 camaras concurrentemente
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    modo_auto = "--auto" in sys.argv

    print("=" * 60)
    print("  EMULADOR DE CAMARAS IP - Sistema Distribuido con Raft")
    print("=" * 60)
    print(f"  Cluster objetivo: {CLUSTER_HOST}  puertos {CLUSTER_PORTS}")
    print(f"  Fotos por camara: {NUM_FOTOS}  |  Pausa: {PAUSA_FOTOS}s")
    print("=" * 60)
    print("\n  AVISO: Asegurate de que el cluster Java este corriendo.\n")

    if not modo_auto:
        input("  Presiona ENTER para iniciar todas las camaras simultaneamente...\n")
    else:
        print("  Modo automatico: iniciando sin esperar ENTER...\n")

    hilos = []
    for camara_id, fotos in DATASETS.items():
        t = threading.Thread(
            target=hilo_camara,
            args=(camara_id, fotos),
            name=f"Thread-{camara_id}",
            daemon=True,
        )
        hilos.append(t)

    # Lanzar todos los hilos al mismo tiempo (concurrencia real)
    for t in hilos:
        t.start()

    # Esperar a que todas las camaras terminen
    for t in hilos:
        t.join()

    print("\n" + "=" * 60)
    print("  Todas las camaras han terminado de transmitir.")
    print("  Revisa la consola del cluster para ver las clasificaciones.")
    print("=" * 60)
