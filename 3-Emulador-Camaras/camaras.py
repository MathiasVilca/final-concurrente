"""
camaras.py — Alias de camera_client.py
Mencionado en el README y el plan de batalla como nombre alternativo.
"""
from camera_client import *

if __name__ == "__main__":
    import camera_client
    camera_client.hilo_camara  # ya importado

    # Reutilizar el main de camera_client
    import sys, os
    sys.argv = [sys.argv[0]]
    exec(open(os.path.join(os.path.dirname(__file__), "camera_client.py")).read())
