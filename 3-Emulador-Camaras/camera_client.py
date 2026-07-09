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
CLUSTER_HOST  = os.environ.get("CLUSTER_HOST", "127.0.0.1")
CLUSTER_PORTS = [8001, 8002, 8003]   # Intentara conectar al lider
NODES         = {"NODE_1": 8001, "NODE_2": 8002, "NODE_3": 8003}
NODE_HOSTS    = {
    "NODE_1": os.environ.get("CLUSTER_NODE_1_HOST", CLUSTER_HOST),
    "NODE_2": os.environ.get("CLUSTER_NODE_2_HOST", CLUSTER_HOST),
    "NODE_3": os.environ.get("CLUSTER_NODE_3_HOST", CLUSTER_HOST),
}
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
    intentados = set()
    endpoints_a_probar = [(NODE_HOSTS[node], port) for node, port in NODES.items()]

    while endpoints_a_probar:
        host, puerto = endpoints_a_probar.pop(0)
        endpoint = (host, puerto)
        if endpoint in intentados:
            continue
        intentados.add(endpoint)

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, puerto))
            print(f"[{camara_id}] Foto {foto_num} -> enviando a {host}:{puerto}: {payload}")
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
                print(f"[{camara_id}] Redirigido por {host}:{puerto}. Buscando lider...")
                partes = respuesta.split("|")
                if len(partes) >= 4 and "LIDER_ES:" in partes[3]:
                    lider = partes[3].replace("LIDER_ES:", "")
                    p = NODES.get(lider)
                    h = NODE_HOSTS.get(lider, CLUSTER_HOST)
                    if p and (h, p) not in intentados:
                        endpoints_a_probar.insert(0, (h, p))
                else:
                    for node, p in NODES.items():
                        h = NODE_HOSTS.get(node, CLUSTER_HOST)
                        if (h, p) not in intentados:
                            endpoints_a_probar.insert(0, (h, p))
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
    if "--host" in sys.argv:
        idx = sys.argv.index("--host")
        if idx + 1 < len(sys.argv):
            CLUSTER_HOST = sys.argv[idx + 1]
            NODE_HOSTS = {node: CLUSTER_HOST for node in NODES}

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
