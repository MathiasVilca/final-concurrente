"""
DÍA 3 - Emulador de Cámaras IP
================================
Simula 3 cámaras IP que envían imágenes (como vectores de características)
al clúster Raft en Java.

Protocolo: CLI_REQ|CAMARA_N|0|color,aspecto,textura\n
           (igual que el protocolo Tipo|Sender|Term|Payload\n del clúster)

Cada cámara corre en su propio hilo y envía 3 "fotos" con 3s de pausa.
"""

import socket
import threading
import time
import random

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────
CLUSTER_HOST  = "127.0.0.1"
CLUSTER_PORTS = [8001, 8002, 8003]   # Intentará conectar al líder
NUM_FOTOS     = 3                    # Fotos por cámara
PAUSA_FOTOS   = 3                    # Segundos entre fotos

# Datasets simulados: cada cámara está calibrada para "ver" un tipo de objeto
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
#  FUNCIÓN DE CONEXIÓN AL CLÚSTER
#  Intenta conectar a cada nodo hasta encontrar el líder
# ─────────────────────────────────────────────────────────────────────

def conectar_al_cluster(timeout=3):
    """
    Devuelve un socket conectado al nodo que acepte la conexión.
    Prueba los puertos en orden.
    """
    for puerto in CLUSTER_PORTS:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((CLUSTER_HOST, puerto))
            return s, puerto
        except (ConnectionRefusedError, socket.timeout):
            continue
    return None, None


def enviar_y_recibir(camara_id, foto_num, vector):
    """
    Abre una conexión al clúster, envía el vector como CLI_REQ y espera CLI_RES.
    Si recibe REDIRECT, reintenta con el nodo correcto automáticamente.
    """
    payload = f"{vector[0]},{vector[1]},{vector[2]}"
    trama   = f"CLI_REQ|{camara_id}|0|{payload}\n"

    # Intentar todos los puertos del clúster hasta obtener CLI_RES
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
            print(f"[{camara_id}] 📸 Foto {foto_num} → enviando a puerto {puerto}: {payload}")
            sock.sendall(trama.encode("utf-8"))

            # Esperar respuesta completa
            sock.settimeout(10)
            respuesta_raw = b""
            while b"\n" not in respuesta_raw:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                respuesta_raw += chunk

            sock.close()
            respuesta = respuesta_raw.decode("utf-8", errors="replace").strip()

            # Si nos redirigen al líder real, añadir sus puerto a la cola
            if respuesta.startswith("REDIRECT"):
                print(f"[{camara_id}] 🔀 Redirigido por puerto {puerto}. Buscando líder...")
                # Agregar puertos restantes al frente para reintento inmediato
                for p in CLUSTER_PORTS:
                    if p not in puertos_intentados:
                        puertos_a_probar.insert(0, p)
                continue

            # Respuesta final CLI_RES
            print(f"[{camara_id}] ✅ Respuesta del clúster: {respuesta}")
            return

        except socket.timeout:
            print(f"[{camara_id}] ⏱️  Timeout en puerto {puerto} para foto {foto_num}.")
        except ConnectionRefusedError:
            print(f"[{camara_id}] ⚡ Nodo en puerto {puerto} no disponible.")
        except Exception as e:
            print(f"[{camara_id}] ❌ Error en puerto {puerto}: {e}")

    print(f"[{camara_id}] ❌ Ningún nodo del clúster respondió CLI_RES para foto {foto_num}.")


# ─────────────────────────────────────────────────────────────────────
#  HILO DE CÁMARA
# ─────────────────────────────────────────────────────────────────────

def hilo_camara(camara_id, fotos):
    """
    Función que corre en su propio Thread.
    Envía 'NUM_FOTOS' imágenes con pausa entre ellas.
    """
    print(f"\n{'='*50}")
    print(f"  [{camara_id}] 📷 Cámara iniciada. Enviará {NUM_FOTOS} fotos.")
    print(f"{'='*50}")

    for i, foto in enumerate(fotos[:NUM_FOTOS], start=1):
        enviar_y_recibir(camara_id, i, foto)
        if i < NUM_FOTOS:
            print(f"[{camara_id}] ⏳ Esperando {PAUSA_FOTOS}s antes de la siguiente foto...")
            time.sleep(PAUSA_FOTOS)

    print(f"[{camara_id}] 🏁 Cámara terminó de enviar todas sus fotos.")


# ─────────────────────────────────────────────────────────────────────
#  MAIN — Lanza las 3 cámaras concurrentemente
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  EMULADOR DE CÁMARAS IP — Sistema Distribuido con Raft")
    print("=" * 60)
    print(f"  Clúster objetivo: {CLUSTER_HOST}  puertos {CLUSTER_PORTS}")
    print(f"  Fotos por cámara: {NUM_FOTOS}  |  Pausa: {PAUSA_FOTOS}s")
    print("=" * 60)
    print("\n⚠️  Asegúrate de que el clúster Java (ClusterLauncher) esté corriendo.\n")

    input("  Presiona ENTER para iniciar todas las cámaras simultáneamente...\n")

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

    # Esperar a que todas las cámaras terminen
    for t in hilos:
        t.join()

    print("\n" + "=" * 60)
    print("  ✅ Todas las cámaras han terminado de transmitir.")
    print("  Revisa la consola del clúster para ver las clasificaciones.")
    print("=" * 60)
