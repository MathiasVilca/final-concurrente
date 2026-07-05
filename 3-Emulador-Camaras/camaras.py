# -*- coding: utf-8 -*-
"""
camaras.py — alias de camera_client.py
Mencionado en el README como nombre alternativo.
"""
import runpy
import sys
import os

if __name__ == "__main__":
    sys.argv = [sys.argv[0]] + [a for a in sys.argv[1:]]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    runpy.run_path(os.path.join(script_dir, "camera_client.py"), run_name="__main__")
