"""Prueba E2E de tolerancia a fallos: apaga el lider y verifica reeleccion."""
import os
import socket
import subprocess
import sys
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CLUSTER_DIR = os.path.join(ROOT_DIR, "2-Cluster-Testeo-Raft")
NODES = {"NODE_1": 8001, "NODE_2": 8002, "NODE_3": 8003}
CAMERAS = {
    "CAMARA_PRE_FAILOVER": [118, 1.22, 47],
    "CAMARA_POST_FAILOVER": [205, 2.48, 12],
}


def puerto_ocupado(puerto):
    try:
        with socket.create_connection(("127.0.0.1", puerto), timeout=0.5):
            return True
    except OSError:
        return False


def verificar_puertos_libres():
    ocupados = [p for p in NODES.values() if puerto_ocupado(p)]
    if not ocupados:
        return True
    print("[ERROR] Hay otro cluster/proceso usando los puertos:", ocupados)
    print("        Cierra el Java anterior antes de ejecutar este test.")
    return False


def limpiar_cluster_lanzado(proc):
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    if os.name != "nt":
        return

    try:
        salida = subprocess.check_output(
            ["netstat", "-ano"],
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception:
        return

    pids = set()
    for linea in salida.splitlines():
        if "LISTENING" not in linea:
            continue
        if any(f":{puerto}" in linea for puerto in NODES.values()):
            partes = linea.split()
            if partes:
                pid = partes[-1]
                if pid.isdigit() and pid != "0":
                    pids.add(pid)

    for pid in pids:
        subprocess.run(
            ["taskkill", "/PID", pid, "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def enviar(camara_id, vector, timeout_total=25):
    payload = f"{vector[0]},{vector[1]},{vector[2]}"
    trama = f"CLI_REQ|{camara_id}|0|{payload}\n"
    deadline = time.time() + timeout_total

    while time.time() < deadline:
        for puerto in NODES.values():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(4)
                    s.connect(("127.0.0.1", puerto))
                    s.sendall(trama.encode("utf-8"))

                    raw = b""
                    s.settimeout(8)
                    while b"\n" not in raw:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                        raw += chunk

                respuesta = raw.decode("utf-8", errors="replace").strip()
                if respuesta.startswith("CLI_RES"):
                    print(f"    [OK] {camara_id} -> {respuesta}")
                    return respuesta
            except OSError:
                pass
        time.sleep(1)

    print(f"    [FALLO] {camara_id} no obtuvo CLI_RES tras {timeout_total}s.")
    return None


def main():
    print("=" * 60)
    print("  TEST FAILOVER E2E - Raft mantiene servicio tras caer lider")
    print("=" * 60)

    if not verificar_puertos_libres():
        sys.exit(1)

    print("[1] Levantando cluster con demo de failover...")
    proc = subprocess.Popen(
        ["java", "-cp", "out", "uTP.ClusterLauncher", "--demo-failover", "--failover-delay-ms=6000"],
        cwd=CLUSTER_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"    PID: {proc.pid}")

    try:
        print("[2] Esperando eleccion inicial...")
        time.sleep(4)
        antes = enviar("CAMARA_PRE_FAILOVER", CAMERAS["CAMARA_PRE_FAILOVER"])

        print("[3] Esperando apagado del lider y nueva eleccion...")
        time.sleep(8)
        despues = enviar("CAMARA_POST_FAILOVER", CAMERAS["CAMARA_POST_FAILOVER"])

        print("\n" + "=" * 60)
        if antes and despues:
            print("  RESULTADO: OK. El cluster respondio antes y despues del failover.")
        else:
            print("  RESULTADO: FALLO. No se pudo confirmar tolerancia a fallos.")
            sys.exit(1)
        print("=" * 60)
    finally:
        limpiar_cluster_lanzado(proc)
        print("Cluster detenido.")


if __name__ == "__main__":
    main()
