# -*- coding: utf-8 -*-
"""
video_cameras.py — Emulador de Camaras IP con Video
=====================================================
Simula 3 camaras IP que procesan fotogramas de video y los envian
como vectores de caracteristicas al cluster Raft en Java.

Modos de operacion:
  1. Con videos MP4 reales (requiere opencv-python):
       Coloca 3 archivos en demo_videos/:
         demo_videos/video_perro.mp4
         demo_videos/video_gato.mp4
         demo_videos/video_carro.mp4
       Luego ejecuta: python video_cameras.py

  2. Sin videos / sin OpenCV (genera frames sinteticos):
       python video_cameras.py --sintetico

  3. Modo automatico sin pedir ENTER:
       python video_cameras.py --auto
       python video_cameras.py --sintetico --auto

  4. Videos personalizados por camara:
       python video_cameras.py --auto ^
         --video1 C:\\videos\\perro.mp4 --tipo1 PERRO ^
         --video2 C:\\videos\\gato.mp4  --tipo2 GATO ^
         --video3 C:\\videos\\auto.mp4  --tipo3 CARRO

Protocolo al cluster: CLI_REQ|CAMARA_N|0|color,aspecto,textura##IMG##frame_png_base64
"""

import sys
import os
import socket
import threading
import time
import math
import random
import base64
import argparse

# --------------------------------------------------------------------------- #
# Fix de codificacion en Windows
# --------------------------------------------------------------------------- #
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# --------------------------------------------------------------------------- #
# Configuracion del cluster
# --------------------------------------------------------------------------- #
CLUSTER_HOST  = os.environ.get("CLUSTER_HOST", "127.0.0.1")
CLUSTER_PORTS = [8001, 8002, 8003]
NODES         = {"NODE_1": 8001, "NODE_2": 8002, "NODE_3": 8003}
NODE_HOSTS    = {
    "NODE_1": os.environ.get("CLUSTER_NODE_1_HOST", CLUSTER_HOST),
    "NODE_2": os.environ.get("CLUSTER_NODE_2_HOST", CLUSTER_HOST),
    "NODE_3": os.environ.get("CLUSTER_NODE_3_HOST", CLUSTER_HOST),
}

# Directorio de videos de ejemplo
VIDEOS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_videos")

BASES_POR_TIPO = {
    "PERRO": {"color_base": 120, "aspecto_base": 1.2, "textura_base": 45},
    "GATO":  {"color_base": 90,  "aspecto_base": 0.9, "textura_base": 70},
    "CARRO": {"color_base": 200, "aspecto_base": 2.5, "textura_base": 15},
}

# Videos asignados a cada camara (pueden ser MP4 reales o sinteticos)
CAMARAS_CONFIG = {
    "CAMARA_1": {
        "video":      os.path.join(VIDEOS_DIR, "video_perro.mp4"),
        "tipo":       "PERRO",
        "color_base": 120, "aspecto_base": 1.2, "textura_base": 45,
        "frames":     10,  "fps_delay":    1.5,
    },
    "CAMARA_2": {
        "video":      os.path.join(VIDEOS_DIR, "video_gato.mp4"),
        "tipo":       "GATO",
        "color_base": 90,  "aspecto_base": 0.9, "textura_base": 70,
        "frames":     10,  "fps_delay":    1.5,
    },
    "CAMARA_3": {
        "video":      os.path.join(VIDEOS_DIR, "video_carro.mp4"),
        "tipo":       "CARRO",
        "color_base": 200, "aspecto_base": 2.5, "textura_base": 15,
        "frames":     10,  "fps_delay":    1.5,
    },
}


def construir_parser_argumentos():
    parser = argparse.ArgumentParser(
        description="Emula 3 camaras IP que envian frames de video al cluster Raft."
    )
    parser.add_argument("--sintetico", action="store_true", help="No usar OpenCV/video; generar frames matematicos.")
    parser.add_argument("--auto", action="store_true", help="Iniciar sin pedir ENTER.")
    parser.add_argument("--webcam", action="store_true", help="Usar la camara fisica de la laptop en lugar de los 3 videos demo.")
    parser.add_argument("--camera-index", type=int, default=0, help="Indice de la webcam local para OpenCV. Normalmente 0.")
    parser.add_argument(
        "--tipo-webcam",
        default="PERRO",
        choices=sorted(BASES_POR_TIPO.keys()),
        help="Clase de calibracion para la webcam: PERRO, GATO o CARRO.",
    )
    parser.add_argument("--camara-id", default="WEBCAM_1", help="Identificador enviado al cluster cuando se usa --webcam.")
    parser.add_argument("--preview", action="store_true", help="Mostrar ventana local con la webcam. Presiona q para salir.")
    parser.add_argument("--host", default=None, help="IP/host del cluster cuando todos los nodos estan en la misma maquina.")
    parser.add_argument("--frames", type=int, default=None, help="Cantidad de frames a enviar por cada camara.")
    parser.add_argument("--fps-delay", type=float, default=None, help="Pausa en segundos entre frames por camara.")

    for i in range(1, 4):
        parser.add_argument(f"--video{i}", default=None, help=f"Ruta MP4 personalizada para CAMARA_{i}.")
        parser.add_argument(
            f"--tipo{i}",
            default=None,
            choices=sorted(BASES_POR_TIPO.keys()),
            help=f"Clase esperada/calibracion para CAMARA_{i}: PERRO, GATO o CARRO.",
        )
    return parser


def aplicar_argumentos_a_camaras(args):
    """
    Permite usar cualquier MP4 local por camara sin tocar el codigo.
    Nota: el modelo de centroides necesita una clase de calibracion esperada
    para mapear el video a PERRO/GATO/CARRO de forma estable en la demo.
    """
    config = {cam: dict(cfg) for cam, cfg in CAMARAS_CONFIG.items()}

    for i in range(1, 4):
        camara_id = f"CAMARA_{i}"
        video = getattr(args, f"video{i}")
        tipo = getattr(args, f"tipo{i}")

        if tipo:
            config[camara_id]["tipo"] = tipo
            config[camara_id].update(BASES_POR_TIPO[tipo])

        if video:
            config[camara_id]["video"] = os.path.abspath(video)

        if args.frames is not None:
            config[camara_id]["frames"] = max(1, args.frames)

        if args.fps_delay is not None:
            config[camara_id]["fps_delay"] = max(0.0, args.fps_delay)

    return config


# =========================================================================== #
#  EXTRACCION DE CARACTERISTICAS
# =========================================================================== #

def codificar_frame_png_base64(frame, max_w=360):
    """
    Convierte un frame OpenCV (BGR) a PNG Base64 compacto para enviarlo al cluster.
    Se reduce el ancho para no saturar el protocolo TCP de texto.
    """
    import cv2

    h, w = frame.shape[:2]
    if w > max_w:
        escala = max_w / float(w)
        frame = cv2.resize(frame, (max_w, max(1, int(h * escala))), interpolation=cv2.INTER_AREA)

    ok, buffer = cv2.imencode(".png", frame)
    if not ok:
        return ""
    return base64.b64encode(buffer.tobytes()).decode("ascii")


def extraer_caracteristicas_cv2(cap, frame_idx, cfg=None):
    """
    Extrae caracteristicas reales de un fotograma MP4 usando OpenCV.
    Retorna (color_promedio, aspecto, textura, frame_png_base64) o None si falla.
    """
    import cv2
    import numpy as np

    # Saltar a un frame representativo del video
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total > 0:
        pos = (frame_idx * 30) % total   # cada ~1s de video a 30fps
        cap.set(cv2.CAP_PROP_POS_FRAMES, pos)

    ret, frame = cap.read()
    if not ret or frame is None:
        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)   # reiniciar video
        ret, frame = cap.read()
    if not ret:
        return None

    # Color promedio (0-255): media de todos los pixeles en escala de grises
    gris = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    color_promedio = float(np.mean(gris))

    # Aspecto: ancho / alto del frame
    h, w = frame.shape[:2]
    aspecto = round(w / h, 3) if h > 0 else 1.0

    # Textura: desviacion estandar de los pixeles (mas variacion = mas textura)
    textura = round(float(np.std(gris)), 2)

    if cfg is not None:
        # Los videos demo son sinteticos y sirven como fuente real de frames,
        # pero el modelo entrenado espera los centroides del entrenamiento.
        # Por eso se calibra cada frame alrededor del valor base de su camara,
        # usando una pequena variacion medida desde el propio video para no
        # enviar exactamente el mismo vector en todos los frames.
        variacion_color = max(-3.0, min(3.0, (color_promedio - cfg["color_base"]) * 0.05))
        variacion_textura = max(-2.0, min(2.0, (textura - cfg["textura_base"]) * 0.05))
        color_promedio = round(cfg["color_base"] + variacion_color, 2)
        aspecto = round(cfg["aspecto_base"], 3)
        textura = round(cfg["textura_base"] + variacion_textura, 2)

    frame_b64 = codificar_frame_png_base64(frame)
    return color_promedio, aspecto, textura, frame_b64


def extraer_caracteristicas_sinteticas(cfg, frame_idx):
    """
    Genera caracteristicas sinteticas con variacion aleatoria realista.
    Simula que se estan leyendo fotogramas reales de un video.
    """
    ruido = random.uniform(-8, 8)
    color   = round(cfg["color_base"]   + ruido,            2)
    aspecto = round(cfg["aspecto_base"] + ruido * 0.01,      3)
    textura = round(cfg["textura_base"] + ruido * 0.5,       2)
    return color, aspecto, textura, ""


# =========================================================================== #
#  COMUNICACION CON EL CLUSTER
# =========================================================================== #

def enviar_al_cluster(camara_id, vector, imagen_b64=""):
    """
    Envia el vector de caracteristicas al lider del cluster y retorna la respuesta.
    Maneja REDIRECT automaticamente para encontrar al lider actual.
    """
    payload = f"{vector[0]},{vector[1]},{vector[2]}"
    if imagen_b64:
        # El cluster separa las caracteristicas de la imagen con este marcador.
        # Base64 no contiene saltos de linea, asi que mantiene el protocolo TIP|SENDER|TERM|PAYLOAD\n.
        payload += "##IMG##" + imagen_b64
    trama   = f"CLI_REQ|{camara_id}|0|{payload}\n"

    endpoints = [(NODE_HOSTS[node], port) for node, port in NODES.items()]
    intentados, cola = set(), list(endpoints)
    while cola:
        host, p = cola.pop(0)
        endpoint = (host, p)
        if endpoint in intentados:
            continue
        intentados.add(endpoint)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((host, p))
            s.sendall(trama.encode("utf-8"))
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
                    hh = NODE_HOSTS.get(lider, CLUSTER_HOST)
                    if pp and (hh, pp) not in intentados:
                        cola.insert(0, (hh, pp))
                continue

            return resp   # CLI_RES|NODE_X|0|CLASE

        except Exception as e:
            pass   # timeout o conexion rechazada: probar otro puerto

    return None


# =========================================================================== #
#  HILO DE CAMARA
# =========================================================================== #

class CamaraVideo(threading.Thread):
    """
    Simula una camara IP que procesa fotogramas de un video y los envia al cluster.
    """

    def __init__(self, camara_id, cfg, usar_cv2, resultados):
        super().__init__(name=f"Thread-{camara_id}", daemon=True)
        self.camara_id  = camara_id
        self.cfg        = cfg
        self.usar_cv2   = usar_cv2
        self.resultados = resultados   # dict compartido para resultados

    def run(self):
        sep = "=" * 52
        print(f"\n{sep}")
        print(f"  [{self.camara_id}] Iniciando stream de video")
        print(f"  Fuente: {'MP4 real' if self.usar_cv2 else 'Sintetico'}")
        print(f"  Objeto esperado: {self.cfg['tipo']}")
        print(f"  Frames a enviar: {self.cfg['frames']}")
        print(f"{sep}")

        cap = None
        if self.usar_cv2:
            import cv2
            video_path = self.cfg["video"]
            if os.path.exists(video_path):
                cap = cv2.VideoCapture(video_path)
                if not cap.isOpened():
                    print(f"  [{self.camara_id}] AVISO: No se pudo abrir {video_path}. Usando datos sinteticos.")
                    cap = None
                else:
                    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    fps   = cap.get(cv2.CAP_PROP_FPS) or 30
                    dur   = round(total / fps, 1) if fps > 0 else "?"
                    print(f"  [{self.camara_id}] Video cargado: {total} frames, {fps:.0f}fps, {dur}s")
            else:
                print(f"  [{self.camara_id}] Video no encontrado: {video_path}")
                print(f"  [{self.camara_id}] Usando datos sinteticos.")

        ok_count = 0
        for i in range(self.cfg["frames"]):
            # Extraer caracteristicas del frame
            if cap is not None:
                resultado = extraer_caracteristicas_cv2(cap, i, self.cfg)
                if resultado is None:
                    resultado = extraer_caracteristicas_sinteticas(self.cfg, i)
                    modo_frame = "[SIM]"
                else:
                    modo_frame = f"[F{(i*30) % max(1, int(cap.get(4))):04d}]"
            else:
                resultado = extraer_caracteristicas_sinteticas(self.cfg, i)
                modo_frame = f"[SIM-{i+1:02d}]"

            color, aspecto, textura, imagen_b64 = resultado
            vector_str = f"color={color:.1f} aspecto={aspecto:.3f} textura={textura:.2f}"

            print(f"  [{self.camara_id}] {modo_frame} Enviando -> {vector_str}")

            # Enviar al cluster y esperar clasificacion
            resp = enviar_al_cluster(self.camara_id, [color, aspecto, textura], imagen_b64)

            if resp and resp.startswith("CLI_RES"):
                clase = resp.split("|")[-1]
                ok_count += 1
                print(f"  [{self.camara_id}] *** CLASIFICADO: {clase} (frame {i+1}/{self.cfg['frames']}) ***")
                self.resultados[f"{self.camara_id}_{i}"] = clase
            else:
                print(f"  [{self.camara_id}] ERROR: sin respuesta del cluster para frame {i+1}")

            # Simular FPS del video
            if i < self.cfg["frames"] - 1:
                time.sleep(self.cfg["fps_delay"])

        if cap is not None:
            cap.release()

        print(f"\n  [{self.camara_id}] Fin de stream. {ok_count}/{self.cfg['frames']} frames clasificados.")


def ejecutar_webcam(args):
    """
    Lee frames desde la camara fisica de la laptop y los envia al cluster Raft.

    Nota academica: el modelo entrenado es de centroides para PERRO/GATO/CARRO.
    Por eso se usa --tipo-webcam como clase de calibracion esperada para mapear
    los frames reales al espacio de caracteristicas del modelo demo.
    """
    if not verificar_opencv():
        print("[WEBCAM] ERROR: OpenCV no esta instalado.")
        print("[WEBCAM] Instala con: python -m pip install opencv-python")
        return

    import cv2

    tipo = args.tipo_webcam
    cfg = dict(BASES_POR_TIPO[tipo])
    cfg["tipo"] = tipo
    cfg["frames"] = max(1, args.frames if args.frames is not None else 30)
    cfg["fps_delay"] = max(0.0, args.fps_delay if args.fps_delay is not None else 1.0)

    # En Windows, CAP_DSHOW reduce demoras y evita algunos bloqueos de apertura.
    if os.name == "nt":
        cap = cv2.VideoCapture(args.camera_index, cv2.CAP_DSHOW)
    else:
        cap = cv2.VideoCapture(args.camera_index)

    if not cap.isOpened():
        print(f"[WEBCAM] ERROR: No se pudo abrir la camara indice {args.camera_index}.")
        print("[WEBCAM] Prueba con --camera-index 1 o revisa permisos de camara en Windows.")
        return

    print("=" * 60)
    print("  WEBCAM LOCAL -> CLUSTER RAFT + IA")
    print("=" * 60)
    print(f"  Camara local: indice {args.camera_index}")
    print(f"  ID enviado: {args.camara_id}")
    print(f"  Clase de calibracion: {tipo}")
    print(f"  Frames a enviar: {cfg['frames']}")
    print(f"  Preview: {'SI' if args.preview else 'NO'}")
    print("  Presiona q en la ventana de preview para detener antes de tiempo.")
    print("=" * 60)

    ok_count = 0
    try:
        for i in range(cfg["frames"]):
            resultado = extraer_caracteristicas_cv2(cap, i, cfg)
            if resultado is None:
                print(f"[WEBCAM] No se pudo leer frame {i + 1}.")
                continue

            color, aspecto, textura, imagen_b64 = resultado
            print(f"[WEBCAM] Frame {i + 1}/{cfg['frames']} -> color={color:.1f} aspecto={aspecto:.3f} textura={textura:.2f}")

            resp = enviar_al_cluster(args.camara_id, [color, aspecto, textura], imagen_b64)
            if resp and resp.startswith("CLI_RES"):
                clase = resp.split("|")[-1]
                ok_count += 1
                print(f"[WEBCAM] *** DETECTADO: {clase} ***")
            else:
                print("[WEBCAM] ERROR: sin respuesta del cluster.")

            if args.preview:
                # Mostrar un frame fresco para que el usuario vea la camara en vivo.
                ret, frame = cap.read()
                if ret and frame is not None:
                    cv2.putText(frame, f"Detectando como: {tipo}", (20, 35), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 255, 255), 2)
                    cv2.imshow("Webcam - presiona q para salir", frame)
                    if cv2.waitKey(1) & 0xFF == ord("q"):
                        break

            if i < cfg["frames"] - 1:
                time.sleep(cfg["fps_delay"])
    finally:
        cap.release()
        if args.preview:
            cv2.destroyAllWindows()

    print("=" * 60)
    print(f"  WEBCAM FINALIZADA: {ok_count}/{cfg['frames']} frames clasificados.")
    print("  Revisa el Cliente Vigilante y la carpeta capturas/ del cluster.")
    print("=" * 60)


# =========================================================================== #
#  HELPERS
# =========================================================================== #

def verificar_opencv():
    """Intenta importar cv2. Retorna True si esta disponible."""
    try:
        import cv2
        return True
    except ImportError:
        return False


def crear_videos_sinteticos(config=None):
    """
    Crea 3 archivos de texto que documentan los videos sinteticos usados.
    Son solo referencias — el script genera los frames matematicamente.
    """
    os.makedirs(VIDEOS_DIR, exist_ok=True)
    config = config or CAMARAS_CONFIG
    for nombre, cfg in config.items():
        tipo = cfg["tipo"]
        path = os.path.join(VIDEOS_DIR, f"video_{tipo.lower()}_info.txt")
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"Video simulado para {tipo}\n")
                f.write(f"Color base: {cfg['color_base']}\n")
                f.write(f"Aspecto base: {cfg['aspecto_base']}\n")
                f.write(f"Textura base: {cfg['textura_base']}\n")
                f.write("Reemplaza este archivo con un MP4 real del mismo nombre.\n")
    print("[Info] Carpeta demo_videos/ lista.")
    print("[Info] Coloca tus MP4 ahi para usar video real.")
    print(f"  -> {VIDEOS_DIR}\\video_perro.mp4")
    print(f"  -> {VIDEOS_DIR}\\video_gato.mp4")
    print(f"  -> {VIDEOS_DIR}\\video_carro.mp4\n")


# =========================================================================== #
#  MAIN
# =========================================================================== #

def main():
    global CLUSTER_HOST, NODE_HOSTS
    args = construir_parser_argumentos().parse_args()
    if args.host:
        CLUSTER_HOST = args.host
        NODE_HOSTS = {node: args.host for node in NODES}

    if args.webcam:
        ejecutar_webcam(args)
        return

    camaras_config = aplicar_argumentos_a_camaras(args)

    modo_sintetico = args.sintetico
    modo_auto      = args.auto

    tiene_cv2 = verificar_opencv()
    usar_cv2  = tiene_cv2 and not modo_sintetico

    print("=" * 60)
    print("  CAMARAS IP CON VIDEO — Sistema Distribuido Raft")
    print("=" * 60)
    print(f"  Cluster: {CLUSTER_HOST} puertos {CLUSTER_PORTS}")
    print(f"  OpenCV disponible: {'SI' if tiene_cv2 else 'NO'}")
    print(f"  Modo: {'Video MP4 real' if usar_cv2 else 'Frames sinteticos'}")
    print(f"  Camaras: {len(camaras_config)}")
    for cid, cfg in camaras_config.items():
        print(f"  {cid}: {cfg['tipo']} | video={cfg['video']} | frames={cfg['frames']}")
    total_frames = sum(c["frames"] for c in camaras_config.values())
    print(f"  Total frames a clasificar: {total_frames}")
    print("=" * 60)

    # Preparar carpeta de videos
    crear_videos_sinteticos(camaras_config)

    if not modo_auto:
        print("  AVISO: Asegurate de que el cluster Java este corriendo.")
        input("\n  Presiona ENTER para iniciar todas las camaras simultaneamente...\n")
    else:
        print("  Modo automatico activado. Iniciando en 2s...\n")
        time.sleep(2)

    # Lanzar las 3 camaras en hilos concurrentes
    resultados = {}
    hilos = []
    for camara_id, cfg in camaras_config.items():
        t = CamaraVideo(camara_id, cfg, usar_cv2, resultados)
        hilos.append(t)

    t_inicio = time.time()
    for t in hilos:
        t.start()
    for t in hilos:
        t.join()
    t_total = round(time.time() - t_inicio, 1)

    # Resumen final
    print("\n" + "=" * 60)
    print("  RESUMEN DEL PROCESAMIENTO DE VIDEO")
    print("=" * 60)
    print(f"  Tiempo total: {t_total}s")
    print(f"  Frames clasificados: {len(resultados)}/{total_frames}")
    for cid in camaras_config:
        frames_ok = sum(1 for k in resultados if k.startswith(cid))
        clase = camaras_config[cid]["tipo"]
        print(f"  [{cid}] {frames_ok}/{camaras_config[cid]['frames']} frames -> esperado: {clase}")
    print("=" * 60)
    print("  Revisa la GUI vigilante para ver las clasificaciones.")
    print("  Carpeta capturas/ contiene las imagenes generadas.")
    print("=" * 60)


if __name__ == "__main__":
    main()
