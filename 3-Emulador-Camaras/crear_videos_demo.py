# -*- coding: utf-8 -*-
"""
crear_videos_demo.py — Genera 3 videos MP4 de demostración
============================================================
Crea 3 videos MP4 con colores/texturas distintos que representan
PERRO, GATO y CARRO. Requiere opencv-python.

Si no tienes OpenCV:
    pip install opencv-python
    python crear_videos_demo.py

Los videos se guardan en demo_videos/ y video_cameras.py los lee
automáticamente.
"""

import sys
import os

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Configuracion de cada "video" sintetico
VIDEOS = {
    "video_perro.mp4": {
        "descripcion": "Perro en parque (colores cafes/amarillos, aspecto cuadrado, textura media)",
        "color_rgb":   (100, 120, 140),   # BGR → tono cafe/gris del pelaje
        "ancho":       640, "alto": 480,   # aspecto ~1.33 (cercano a 1.2)
        "duracion_s":  8,   "fps": 15,
        "ruido":       30,                 # variacion de textura
    },
    "video_gato.mp4": {
        "descripcion": "Gato en interior (tonos oscuros, aspecto casi cuadrado, alta textura)",
        "color_rgb":   (70, 90, 110),      # tono oscuro del pelaje del gato
        "ancho":       480, "alto": 480,   # aspecto = 1.0 (cercano a 0.9)
        "duracion_s":  8,   "fps": 15,
        "ruido":       55,                 # mas variacion (pelo del gato)
    },
    "video_carro.mp4": {
        "descripcion": "Carro en calle (colores claros/grises, aspecto panoramico, baja textura)",
        "color_rgb":   (180, 200, 210),    # tono gris/blanco del carroceria
        "ancho":       960, "alto": 480,   # aspecto = 2.0 (cercano a 2.5)
        "duracion_s":  8,   "fps": 15,
        "ruido":       10,                 # poca variacion (superficie lisa del carro)
    },
}


def generar_frame(ancho, alto, color_base, frame_idx, ruido_max, rng):
    """
    Genera un frame numpy simulando el objeto indicado.
    Agrega ruido para simular movimiento real del video.
    """
    import numpy as np

    # Frame base con el color del objeto
    frame = np.full((alto, ancho, 3), color_base, dtype=np.uint8)

    # Agregar ruido aleatorio para simular textura real
    noise = rng.randint(-ruido_max, ruido_max, (alto, ancho, 3), dtype=np.int16)
    frame = np.clip(frame.astype(np.int16) + noise, 0, 255).astype(np.uint8)

    # Agregar un rectangulo que "se mueve" (simula al animal/objeto en movimiento)
    x = (frame_idx * 15) % (ancho - 120)
    y = int(alto * 0.3)
    color_rect = tuple(int(c * 0.6) for c in color_base)
    frame[y : y + 80, x : x + 100] = color_rect

    # Texto identificador del video
    return frame


def crear_video(nombre_archivo, cfg, output_dir):
    try:
        import cv2
        import numpy as np
    except ImportError:
        print("ERROR: OpenCV no instalado. Ejecuta: pip install opencv-python")
        return False

    os.makedirs(output_dir, exist_ok=True)
    ruta = os.path.join(output_dir, nombre_archivo)

    w, h   = cfg["ancho"], cfg["alto"]
    fps    = cfg["fps"]
    frames = cfg["duracion_s"] * fps

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(ruta, fourcc, fps, (w, h))

    if not writer.isOpened():
        print(f"  ERROR: No se pudo crear {ruta}")
        return False

    rng = np.random.default_rng(42)
    print(f"  Generando {nombre_archivo}  ({w}x{h}, {fps}fps, {cfg['duracion_s']}s)...")
    for i in range(frames):
        frame = generar_frame(w, h, cfg["color_rgb"], i, cfg["ruido"], rng)
        # Agregar texto con cv2
        cv2.putText(frame, cfg["descripcion"][:40], (10, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        cv2.putText(frame, f"Frame {i+1}/{frames}", (10, h - 15),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (200, 200, 200), 1)
        writer.write(frame)

    writer.release()
    size_kb = os.path.getsize(ruta) // 1024
    print(f"    -> {ruta}  ({size_kb} KB)  OK")
    return True


def main():
    print("=" * 60)
    print("  GENERADOR DE VIDEOS DE DEMO")
    print("=" * 60)

    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_videos")
    print(f"  Destino: {output_dir}\n")

    ok = 0
    for nombre, cfg in VIDEOS.items():
        if crear_video(nombre, cfg, output_dir):
            ok += 1

    print(f"\n{ok}/{len(VIDEOS)} videos generados.")
    if ok == len(VIDEOS):
        print("\n[OK] Ahora ejecuta:")
        print("       python video_cameras.py")
        print("     para usar los videos reales con el cluster.")
    else:
        print("\nPara instalar OpenCV:")
        print("  pip install opencv-python")
        print("\nSin OpenCV usa el modo sintetico:")
        print("  python video_cameras.py --sintetico")


if __name__ == "__main__":
    main()
