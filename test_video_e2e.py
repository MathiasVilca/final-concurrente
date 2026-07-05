"""test_video_e2e.py - Prueba completa con camaras de video sintetico"""
import subprocess, socket, threading, time, os, sys

CLUSTER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "2-Cluster-Testeo-Raft")
CAMERA_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "3-Emulador-Camaras")
NODES       = {"NODE_1": 8001, "NODE_2": 8002, "NODE_3": 8003}


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
    """Apaga el cluster Java de la prueba y libera 8001/8002/8003 en Windows."""
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

def esperar_puerto(timeout=10):
    for _ in range(timeout * 2):
        for p in [8001, 8002, 8003]:
            try:
                s = socket.socket(); s.settimeout(0.5); s.connect(("127.0.0.1", p)); s.close(); return True
            except: pass
        time.sleep(0.5)
    return False

def main():
    print("=" * 60)
    print("  TEST VIDEO E2E — Camaras + Cluster Raft")
    print("=" * 60)

    # Levantar cluster
    if not verificar_puertos_libres():
        sys.exit(1)

    print("[1] Levantando cluster Java...")
    proc = subprocess.Popen(
        ["java", "-cp", "out", "uTP.ClusterLauncher"],
        cwd=CLUSTER_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"    PID: {proc.pid}")

    try:
        # Esperar lider
        print("[2] Esperando lider (8s)...")
        time.sleep(8)
        print("    Listo.")

        # Ejecutar video_cameras.py con los MP4 reales de demo_videos/.
        # Si falta OpenCV o algun video, video_cameras.py cae a modo sintetico
        # por camara para que la demo no se rompa.
        print("[3] Lanzando camaras de video reales desde demo_videos/...")
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        cam_proc = subprocess.run(
            [sys.executable, "video_cameras.py", "--auto"],
            cwd=CAMERA_DIR,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
        )
        print(cam_proc.stdout)
        if cam_proc.returncode != 0 and cam_proc.stderr:
            print("STDERR:", cam_proc.stderr[:300])

        # Verificar capturas
        print("[4] Verificando capturas generadas...")
        capturas_dir = os.path.join(CLUSTER_DIR, "capturas")
        if os.path.isdir(capturas_dir):
            pngs = sorted([f for f in os.listdir(capturas_dir) if f.endswith(".png")])
            print(f"    Total archivos .png en capturas/: {len(pngs)}")
            # Mostrar los 6 mas recientes
            for f in pngs[-6:]:
                kb = os.path.getsize(os.path.join(capturas_dir, f)) // 1024
                print(f"    {f}  ({kb}KB)")
    finally:
        limpiar_cluster_lanzado(proc)
        print("\nCluster detenido. Prueba completada.")

if __name__ == "__main__":
    main()
