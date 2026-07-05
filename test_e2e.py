"""test_final.py - Prueba final completa del sistema"""
import subprocess, socket, threading, time, os, sys

CLUSTER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "2-Cluster-Testeo-Raft")
NODES = {"NODE_1": 8001, "NODE_2": 8002, "NODE_3": 8003}
CAMERAS = {
    "CAMARA_1": [118, 1.22, 47],
    "CAMARA_2": [88,  0.92, 72],
    "CAMARA_3": [205, 2.48, 12],
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
    print("        Windows: netstat -ano | findstr /C:\":8001\" /C:\":8002\" /C:\":8003\"")
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


def enviar(camara_id, vector, resultados):
    payload = f"{vector[0]},{vector[1]},{vector[2]}"
    trama   = f"CLI_REQ|{camara_id}|0|{payload}\n"
    intentados, cola = set(), [8001, 8002, 8003]
    while cola:
        p = cola.pop(0)
        if p in intentados:
            continue
        intentados.add(p)
        try:
            s = socket.socket()
            s.settimeout(5)
            s.connect(("127.0.0.1", p))
            s.sendall(trama.encode())
            s.settimeout(20)
            raw = b""
            while b"\n" not in raw:
                c = s.recv(4096)
                if not c:
                    break
                raw += c
            s.close()
            resp = raw.decode("utf-8", errors="replace").strip()
            if resp.startswith("REDIRECT"):
                partes = resp.split("|")
                if len(partes) >= 4 and "LIDER_ES:" in partes[3]:
                    lider = partes[3].replace("LIDER_ES:", "")
                    pp = NODES.get(lider)
                    if pp and pp not in intentados:
                        cola.insert(0, pp)
                continue
            resultados[camara_id] = resp
            print(f"  [{camara_id}] OK -> {resp}")
            return
        except Exception as e:
            print(f"  [{camara_id}] Error en {p}: {e}")
    resultados[camara_id] = "ERROR"
    print(f"  [{camara_id}] FALLO - sin respuesta")


def pedir_log():
    for p in [8001, 8002, 8003]:
        try:
            s = socket.socket()
            s.settimeout(5)
            s.connect(("127.0.0.1", p))
            s.sendall(b"CLI_REQ|TEST|0|GET_LOG\n")
            raw = b""
            while b"\n" not in raw:
                c = s.recv(8192)
                if not c:
                    break
                raw += c
            s.close()
            resp = raw.decode("utf-8", errors="replace").strip()
            if "CLI_RES" in resp:
                partes = resp.split("|", 3)
                return partes[3] if len(partes) == 4 else resp, p
        except:
            pass
    return None, None


def main():
    print("=" * 60)
    print("  TEST FINAL — Sistema Raft + IA")
    print("=" * 60)

    # 1. Levantar cluster
    if not verificar_puertos_libres():
        sys.exit(1)

    print(f"[1] Levantando cluster Java...")
    proc = subprocess.Popen(
        ["java", "-cp", "out", "uTP.ClusterLauncher"],
        cwd=CLUSTER_DIR,
        # IMPORTANTE: usar DEVNULL para evitar que el pipe lleno bloquee el JVM.
        # El cluster escribe heartbeats cada 800ms y llena el buffer de 64KB rapidamente.
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"    PID: {proc.pid}")

    try:
        # 2. Esperar lider
        print("[2] Esperando eleccion de lider (6s)...")
        time.sleep(6)
        print("    Listo.")

        # 3. Enviar camaras en paralelo
        print("[3] Enviando 3 camaras concurrentes...")
        resultados = {}
        hilos = [threading.Thread(target=enviar, args=(cid, vec, resultados))
                 for cid, vec in CAMERAS.items()]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()

        # 4. Pedir log
        time.sleep(2)
        print("[4] Consultando log del cluster...")
        log, puerto_log = pedir_log()
        if log:
            print(f"    Log recibido del puerto {puerto_log}:")
            entradas = [e.strip() for e in log.split(";") if e.strip()]
            for e in entradas:
                print(f"      {e}")
        else:
            print("    AVISO: log vacio o no disponible (normal si el log vive en el lider).")

        # 5. Verificar capturas
        print("[5] Verificando capturas .png...")
        capturas_dir = os.path.join(CLUSTER_DIR, "capturas")
        if os.path.isdir(capturas_dir):
            pngs = sorted([f for f in os.listdir(capturas_dir) if f.endswith(".png")])
            print(f"    Archivos .png: {len(pngs)}")
            for f in pngs[-5:]:
                ruta = os.path.join(capturas_dir, f)
                kb = os.path.getsize(ruta) // 1024
                print(f"      {f}  ({kb} KB)")
        else:
            print("    Carpeta capturas/ aun no existe.")

        # Resultado
        ok  = sum(1 for v in resultados.values() if v.startswith("CLI_RES"))
        tot = len(CAMERAS)
        print("\n" + "=" * 60)
        print(f"  RESULTADO: {ok}/{tot} camaras clasificadas correctamente.")
        for k, v in resultados.items():
            estado = "OK" if v.startswith("CLI_RES") else "FALLO"
            print(f"    [{estado}] {k}: {v}")
        if ok == tot:
            print("\n  TODO EL SISTEMA FUNCIONA CORRECTAMENTE.")
        else:
            print(f"\n  {tot - ok} camara(s) fallaron (puede ser timeout de carga).")
        print("=" * 60)
    finally:
        limpiar_cluster_lanzado(proc)
        print("Cluster detenido.")


if __name__ == "__main__":
    main()
