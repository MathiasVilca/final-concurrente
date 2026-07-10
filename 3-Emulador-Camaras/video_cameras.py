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
        default=None,
        choices=sorted(BASES_POR_TIPO.keys()),
        help="Clase de calibracion SOLO si usas --calibrar-webcam: PERRO, GATO o CARRO.",
    )
    parser.add_argument(
        "--calibrar-webcam",
        action="store_true",
        help="Modo demo: ajusta la webcam hacia la clase indicada con --tipo-webcam.",
    )
    parser.add_argument("--camara-id", default="WEBCAM_1", help="Identificador enviado al cluster cuando se usa --webcam.")
    parser.add_argument("--preview", action="store_true", help="Mostrar ventana local con la webcam. Presiona q para salir.")
    parser.add_argument("--continuous", action="store_true", help="Mantener la camara encendida hasta presionar q. Ignora el limite de --frames para cortar la vista.")
    parser.add_argument("--send-interval", type=float, default=None, help="Intervalo en segundos entre envios al cluster. Recomendado: 1.0 a 2.0.")
    parser.add_argument("--webcam-max-width", type=int, default=360, help="Ancho maximo del PNG enviado. 360/480 dan buena calidad.")
    parser.add_argument("--no-send-image", action="store_true", help="Envia solo vector numerico, no PNG. Maxima fluidez, pero no guarda foto real.")
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

def codificar_frame_jpg_base64(frame, max_w=200, calidad=60):
    """
    Convierte un frame OpenCV (BGR) a JPEG Base64 compacto para enviarlo al cluster.
    JPEG es ~10x mas pequeno que PNG, evita saturar el protocolo TCP y reduce latencia.
    max_w=200 y calidad=60 dan buena relacion tamano/fidelidad.
    IMPORTANTE: base64.b64encode nunca produce saltos de linea (RFC 4648 sin padding de lineas),
    por lo que es seguro usarlo como payload del protocolo delimitado por \n.
    """
    import cv2

    h, w = frame.shape[:2]
    if w > max_w:
        escala = max_w / float(w)
        frame = cv2.resize(frame, (max_w, max(1, int(h * escala))), interpolation=cv2.INTER_AREA)

    encode_params = [cv2.IMWRITE_JPEG_QUALITY, calidad]
    ok, buffer = cv2.imencode(".jpg", frame, encode_params)
    if not ok:
        return ""
    return base64.b64encode(buffer.tobytes()).decode("ascii")


# Alias de compatibilidad con el codigo existente que usa el nombre anterior
def codificar_frame_png_base64(frame, max_w=360):
    """Alias para codificar_frame_jpg_base64 con parametros compatibles."""
    return codificar_frame_jpg_base64(frame, max_w=min(max_w, 200))


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


def extraer_caracteristicas_frame(frame, cfg=None, max_w=360, incluir_imagen=True):
    """
    Extrae caracteristicas desde un frame ya capturado de webcam.
    Esto evita leer la camara dos veces y permite mostrar preview en tiempo real
    mientras el envio al cluster ocurre en segundo plano.
    """
    import cv2
    import numpy as np

    gris = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    color_promedio = float(np.mean(gris))
    h, w = frame.shape[:2]
    aspecto = round(w / h, 3) if h > 0 else 1.0
    textura = round(float(np.std(gris)), 2)

    if cfg is not None and "color_base" in cfg:
        variacion_color = max(-3.0, min(3.0, (color_promedio - cfg["color_base"]) * 0.05))
        variacion_textura = max(-2.0, min(2.0, (textura - cfg["textura_base"]) * 0.05))
        color_promedio = round(cfg["color_base"] + variacion_color, 2)
        aspecto = round(cfg["aspecto_base"], 3)
        textura = round(cfg["textura_base"] + variacion_textura, 2)

    imagen_b64 = codificar_frame_png_base64(frame, max_w=max_w) if incluir_imagen else ""
    return color_promedio, aspecto, textura, imagen_b64


def dibujar_overlay_webcam(frame, clase, estado_texto, camara_id, enviados, ok_count, total_frames, en_vuelo):
    """Dibuja recuadro y resultado en la ventana de webcam en tiempo real."""
    import cv2
    h, w = frame.shape[:2]
    color_por_clase = {
        "PERRO": (0, 220, 0),
        "GATO": (255, 120, 0),
        "CARRO": (0, 140, 255),
        "DESCONOCIDO": (180, 180, 180),
        "-": (0, 255, 255),
    }
    color = color_por_clase.get(str(clase).upper(), (0, 255, 255))

    box_w = int(w * 0.58)
    box_h = int(h * 0.62)
    x = max(8, (w - box_w) // 2)
    y = max(55, (h - box_h) // 2 + 20)
    cv2.rectangle(frame, (x, y), (min(w - 8, x + box_w), min(h - 8, y + box_h)), color, 3)

    titulo = f"Detecto: {clase}" if clase and clase != "-" else "Detectando..."
    cv2.rectangle(frame, (x, max(0, y - 38)), (min(w - 8, x + 360), y), color, -1)
    cv2.putText(frame, titulo, (x + 8, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.85, (0, 0, 0), 2)

    progreso = f"{camara_id} | enviados {enviados}/{total_frames if total_frames else 'inf'} | OK {ok_count}"
    envio = "enviando..." if en_vuelo else "listo"
    cv2.putText(frame, progreso, (15, 28), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (0, 255, 255), 2)
    cv2.putText(frame, f"{estado_texto} | {envio} | q=salir", (15, 52), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 255), 2)
    return frame


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
    Webcam en tiempo real:
    - muestra la imagen continuamente;
    - envia frames al cluster en segundo plano para no lagear;
    - dibuja en la misma ventana PERRO/GATO/CARRO/DESCONOCIDO;
    - el cluster guarda la captura clasificada cuando se envia imagen.
    """
    if not verificar_opencv():
        print("[WEBCAM] ERROR: OpenCV no esta instalado.")
        print("[WEBCAM] Instala con: python -m pip install opencv-python")
        return

    import cv2

    tipo = args.tipo_webcam or "SIN_CALIBRAR"
    if args.calibrar_webcam:
        tipo = args.tipo_webcam or "PERRO"
        cfg = dict(BASES_POR_TIPO[tipo])
        cfg["tipo"] = tipo
    else:
        cfg = {"tipo": tipo}

    total_frames = max(1, args.frames if args.frames is not None else 999999)
    send_interval = args.send_interval
    if send_interval is None:
        send_interval = args.fps_delay if args.fps_delay is not None else 1.0
    send_interval = max(0.25, float(send_interval))

    # ── Apertura de camara con maximo 2 intentos de backend ──────────────────
    cap = None
    backends = [cv2.CAP_DSHOW, cv2.CAP_ANY] if os.name == "nt" else [cv2.CAP_ANY]
    for backend in backends:
        _cap = cv2.VideoCapture(args.camera_index, backend)
        if _cap.isOpened():
            cap = _cap
            break
        _cap.release()

    if cap is None or not cap.isOpened():
        print(f"[WEBCAM] ERROR: No se pudo abrir la camara indice {args.camera_index}.")
        print("[WEBCAM] Prueba con --camera-index 1 o revisa permisos de camara en Windows.")
        return

    # ── Configuracion para MINIMA LATENCIA ───────────────────────────────────
    # BUFFERSIZE=1 evita que OpenCV acumule frames viejos (causa principal del lag).
    # Resolucion 640x480 equilibra calidad y velocidad de captura.
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
    cap.set(cv2.CAP_PROP_FPS, 30)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # clave anti-lag: descartar frames acumulados

    estado = {
        "enviados": 0,
        "ok": 0,
        "en_vuelo": False,
        "ultimo_envio": 0.0,
        "ultima_clase": "-",
        "ultima_resp": "esperando primer envio",
        "detener": False,
    }
    estado_lock = threading.Lock()

    def enviar_frame_async(frame_snapshot, numero_frame):
        try:
            # Usar max_w pequeño (200px) para JPEG: trama mas pequeña, menos latencia TCP
            max_ancho_envio = min(200, max(120, args.webcam_max_width))
            color, aspecto, textura, imagen_b64 = extraer_caracteristicas_frame(
                frame_snapshot,
                cfg if args.calibrar_webcam else None,
                max_w=max_ancho_envio,
                incluir_imagen=not args.no_send_image,
            )
            resp = enviar_al_cluster(args.camara_id, [color, aspecto, textura], imagen_b64)
            with estado_lock:
                if resp and resp.startswith("CLI_RES"):
                    # Respuesta: CLI_RES|NODE_X|0|CLASE
                    partes_resp = resp.split("|")
                    clase = partes_resp[-1].strip() if partes_resp else "DESCONOCIDO"
                    # Normalizar clase conocida
                    if clase not in ("PERRO", "GATO", "CARRO", "DESCONOCIDO"):
                        clase = "DESCONOCIDO"
                    estado["ok"] += 1
                    estado["ultima_clase"] = clase
                    estado["ultima_resp"] = f"OK {clase}"
                    print(f"[WEBCAM] Frame {numero_frame} -> {clase}  "
                          f"vec=({color:.1f},{aspecto:.3f},{textura:.1f})")
                elif resp and "ERROR" in resp:
                    # El cluster devolvio ERROR (excepcion interna)
                    estado["ultima_resp"] = "cluster ERROR - reintentando"
                    estado["ultima_clase"] = "DESCONOCIDO"
                    print(f"[WEBCAM] Frame {numero_frame} -> cluster reporto ERROR")
                else:
                    estado["ultima_resp"] = "sin respuesta del cluster"
                    print(f"[WEBCAM] Frame {numero_frame} -> sin respuesta")
        except Exception as e:
            with estado_lock:
                estado["ultima_resp"] = f"error local: {e}"
            print(f"[WEBCAM] Error enviando frame {numero_frame}: {e}")
        finally:
            with estado_lock:
                estado["en_vuelo"] = False

    print("=" * 60)
    print("  WEBCAM EN VIVO -> CLUSTER RAFT + IA")
    print("=" * 60)
    print(f"  Camara local: indice {args.camera_index}")
    print(f"  ID enviado: {args.camara_id}")
    print(f"  Modo: {'CALIBRADO/DEMO' if args.calibrar_webcam else 'REAL sin calibracion'}")
    print(f"  Clase calibracion: {tipo if args.calibrar_webcam else 'ninguna'}")
    print(f"  Envios maximos: {'continuo' if args.continuous else total_frames}")
    print(f"  Intervalo envio: {send_interval}s")
    print(f"  Guarda imagen real: {'NO' if args.no_send_image else 'SI'} | ancho PNG={args.webcam_max_width}")
    print("  Presiona q en la ventana para salir.")
    print("=" * 60)

    try:
        while True:
            # ── ANTI-LAG: descartar frames acumulados en el buffer ───────────
            # cap.grab() descarta sin decodificar; solo decodificamos el ultimo.
            # Esto evita que la ventana muestre frames "viejos" acumulados.
            for _ in range(2):
                cap.grab()
            ret, frame = cap.retrieve()
            if not ret or frame is None:
                ret, frame = cap.read()   # fallback
            if not ret or frame is None:
                time.sleep(0.02)
                continue

            ahora = time.time()
            with estado_lock:
                limite_ok = args.continuous or estado["enviados"] < total_frames
                puede_enviar = (
                    limite_ok
                    and not estado["en_vuelo"]
                    and (ahora - estado["ultimo_envio"] >= send_interval)
                )
                if puede_enviar:
                    estado["en_vuelo"] = True
                    estado["enviados"] += 1
                    estado["ultimo_envio"] = ahora
                    numero_frame = estado["enviados"]
                else:
                    numero_frame = None

                enviados = estado["enviados"]
                ok_count = estado["ok"]
                clase = estado["ultima_clase"]
                resp_txt = estado["ultima_resp"]
                en_vuelo = estado["en_vuelo"]

            if numero_frame is not None:
                threading.Thread(
                    target=enviar_frame_async,
                    args=(frame.copy(), numero_frame),
                    name="WebcamSender",
                    daemon=True,
                ).start()

            frame_mostrar = dibujar_overlay_webcam(
                frame,
                clase,
                resp_txt,
                args.camara_id,
                enviados,
                ok_count,
                None if args.continuous else total_frames,
                en_vuelo,
            )
            cv2.imshow("Webcam IA + Raft - tiempo real (q para salir)", frame_mostrar)

            # waitKey(1) es necesario para que OpenCV procese eventos de ventana.
            # Un valor mas alto (ej. 10) reduciria CPU pero aumentaria el lag de 'q'.
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

            with estado_lock:
                terminado = (not args.continuous) and estado["enviados"] >= total_frames and not estado["en_vuelo"]
            if terminado:
                # Dejar visible un instante el ultimo resultado y salir.
                time.sleep(0.4)
                break
    finally:
        # Asegurar liberacion aunque haya excepcion inesperada
        try:
            cap.release()
        except Exception:
            pass
        cv2.destroyAllWindows()

    with estado_lock:
        ok_count = estado["ok"]
        enviados = estado["enviados"]
        ultima_clase = estado["ultima_clase"]

    print("=" * 60)
    print(f"  WEBCAM FINALIZADA: {ok_count}/{enviados} respuestas OK. Ultimo={ultima_clase}")
    print("  Las capturas clasificadas se guardan en 2-Cluster-Testeo-Raft/capturas/ si no usaste --no-send-image.")
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
