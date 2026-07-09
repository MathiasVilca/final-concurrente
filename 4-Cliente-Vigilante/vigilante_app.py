"""
Cliente Vigilante (Tkinter)
Consulta el cluster Raft por sockets TCP y muestra el registro de detecciones.
"""

import os
import socket
import threading
import time
import tkinter as tk
import math
from tkinter import ttk

CLUSTER_HOST = os.environ.get("CLUSTER_HOST", "127.0.0.1")
CLUSTER_PORTS = [8001, 8002, 8003]
NODES = {"NODE_1": 8001, "NODE_2": 8002, "NODE_3": 8003}
NODE_HOSTS = {
    "NODE_1": os.environ.get("CLUSTER_NODE_1_HOST", CLUSTER_HOST),
    "NODE_2": os.environ.get("CLUSTER_NODE_2_HOST", CLUSTER_HOST),
    "NODE_3": os.environ.get("CLUSTER_NODE_3_HOST", CLUSTER_HOST),
}
INTERVALO_REFRESCO = 5

COLORES_CLASE = {
    "PERRO": "#2ecc71",
    "GATO": "#3498db",
    "CARRO": "#e67e22",
    "DESCONOCIDO": "#95a5a6",
}


def pedir_log_al_cluster(timeout=5):
    trama = "CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG\n"

    for node_id, puerto in NODES.items():
        host = NODE_HOSTS.get(node_id, CLUSTER_HOST)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((host, puerto))
                s.sendall(trama.encode("utf-8"))

                respuesta_raw = b""
                while b"\n" not in respuesta_raw:
                    chunk = s.recv(8192)
                    if not chunk:
                        break
                    respuesta_raw += chunk

                respuesta = respuesta_raw.decode("utf-8", errors="replace").strip()
                partes = respuesta.split("|", 3)
                if len(partes) == 4 and partes[0] == "CLI_RES":
                    payload = partes[3]
                    nodo_respuesta = f"{partes[1]} ({host}:{puerto})"
                    if payload == "LOG_VACIO":
                        return [], nodo_respuesta
                    entradas = [e.strip() for e in payload.split(";") if e.strip()]
                    return entradas, nodo_respuesta
        except Exception:
            continue

    return None, None


def parsear_entrada(entrada_str):
    partes = [p.strip() for p in entrada_str.split("|")]
    if len(partes) >= 4:
        return {"camara": partes[0], "clase": partes[1], "fecha": partes[2], "imagen": partes[3]}
    if len(partes) >= 3:
        return {"camara": partes[0], "clase": partes[1], "fecha": partes[2], "imagen": ""}
    if len(partes) == 2:
        return {"camara": partes[0], "clase": partes[1], "fecha": "-", "imagen": ""}
    return {"camara": entrada_str, "clase": "?", "fecha": "-", "imagen": ""}


class VigilanteApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Cliente Vigilante - Cluster Raft + IA")
        self.root.geometry("1050x650")
        self.root.configure(bg="#1a1a2e")
        self.root.resizable(True, True)

        self._auto_refresh = False
        self._ultimo_nodo = "-"
        self._imagenes_por_item = {}
        self._preview_img = None
        self._construir_ui()

    def _construir_ui(self):
        cabecera = tk.Frame(self.root, bg="#16213e", pady=10)
        cabecera.pack(fill=tk.X)

        tk.Label(
            cabecera,
            text="Cliente Vigilante - Sistema Distribuido con Raft + IA",
            font=("Segoe UI", 14, "bold"),
            fg="#e0e0e0",
            bg="#16213e",
        ).pack(side=tk.LEFT, padx=20)

        self.lbl_nodo = tk.Label(
            cabecera,
            text="Nodo: -",
            font=("Segoe UI", 10),
            fg="#f39c12",
            bg="#16213e",
        )
        self.lbl_nodo.pack(side=tk.RIGHT, padx=20)

        barra = tk.Frame(self.root, bg="#0f3460", pady=8)
        barra.pack(fill=tk.X)

        tk.Label(
            barra,
            text=f"Host: {CLUSTER_HOST}   Puertos: {CLUSTER_PORTS}",
            font=("Segoe UI", 9),
            fg="#bdc3c7",
            bg="#0f3460",
        ).pack(side=tk.LEFT, padx=10)

        self.btn_refrescar = tk.Button(
            barra,
            text="Refrescar Ahora",
            command=self._refrescar_manual,
            bg="#e74c3c",
            fg="white",
            font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT,
            padx=12,
            pady=4,
            cursor="hand2",
        )
        self.btn_refrescar.pack(side=tk.RIGHT, padx=8)

        self.btn_auto = tk.Button(
            barra,
            text=f"Auto-Refresh ({INTERVALO_REFRESCO}s)",
            command=self._toggle_auto_refresh,
            bg="#27ae60",
            fg="white",
            font=("Segoe UI", 9, "bold"),
            relief=tk.FLAT,
            padx=12,
            pady=4,
            cursor="hand2",
        )
        self.btn_auto.pack(side=tk.RIGHT, padx=4)

        tk.Button(
            barra,
            text="Limpiar",
            command=self._limpiar_tabla,
            bg="#7f8c8d",
            fg="white",
            font=("Segoe UI", 9),
            relief=tk.FLAT,
            padx=8,
            pady=4,
            cursor="hand2",
        ).pack(side=tk.RIGHT, padx=4)

        contenedor = tk.Frame(self.root, bg="#1a1a2e")
        contenedor.pack(fill=tk.BOTH, expand=True, padx=16, pady=(12, 0))

        columnas = ("#", "Camara", "Clasificacion IA", "Fecha / Hora", "Archivo PNG")
        self.tabla = ttk.Treeview(contenedor, columns=columnas, show="headings", height=6)

        estilo = ttk.Style()
        estilo.theme_use("clam")
        estilo.configure(
            "Treeview",
            background="#16213e",
            foreground="#ecf0f1",
            rowheight=32,
            fieldbackground="#16213e",
            font=("Segoe UI", 10),
        )
        estilo.configure(
            "Treeview.Heading",
            background="#0f3460",
            foreground="#f39c12",
            font=("Segoe UI", 10, "bold"),
        )
        estilo.map("Treeview", background=[("selected", "#e74c3c")])

        anchos = {
            "#": 50,
            "Camara": 130,
            "Clasificacion IA": 170,
            "Fecha / Hora": 190,
            "Archivo PNG": 420,
        }

        for col in columnas:
            self.tabla.heading(col, text=col)
            self.tabla.column(col, width=anchos.get(col, 150), anchor=tk.CENTER)

        scroll_y = ttk.Scrollbar(contenedor, orient=tk.VERTICAL, command=self.tabla.yview)
        self.tabla.configure(yscrollcommand=scroll_y.set)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.tabla.pack(fill=tk.BOTH, expand=True)
        self.tabla.bind("<<TreeviewSelect>>", self._mostrar_imagen_seleccionada)

        for clase, color in COLORES_CLASE.items():
            self.tabla.tag_configure(clase, foreground=color)

        preview_frame = tk.Frame(contenedor, bg="#0f3460", pady=8)
        preview_frame.pack(fill=tk.X, pady=(10, 0))

        self.lbl_preview = tk.Label(
            preview_frame,
            text="Vista previa: seleccione un registro",
            font=("Segoe UI", 10, "bold"),
            fg="#f39c12",
            bg="#0f3460",
            compound=tk.LEFT,
            padx=8,
            pady=8,
        )
        self.lbl_preview.pack(side=tk.LEFT)

        self.lbl_preview_info = tk.Label(
            preview_frame,
            text="",
            font=("Segoe UI", 9),
            fg="#bdc3c7",
            bg="#0f3460",
            padx=8,
            pady=8,
        )
        self.lbl_preview_info.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.barra_estado = tk.Frame(self.root, bg="#16213e", pady=6)
        self.barra_estado.pack(fill=tk.X, side=tk.BOTTOM)

        self.lbl_estado = tk.Label(
            self.barra_estado,
            text="Listo. Presiona 'Refrescar Ahora' para consultar el cluster.",
            font=("Segoe UI", 9),
            fg="#2ecc71",
            bg="#16213e",
            anchor=tk.W,
        )
        self.lbl_estado.pack(side=tk.LEFT, padx=10)

        self.lbl_conteo = tk.Label(
            self.barra_estado,
            text="Total: 0 registros",
            font=("Segoe UI", 9),
            fg="#bdc3c7",
            bg="#16213e",
        )
        self.lbl_conteo.pack(side=tk.RIGHT, padx=10)

    def _refrescar_manual(self):
        self._set_estado("Consultando el cluster...", "#f39c12")
        threading.Thread(target=self._tarea_refresco, daemon=True).start()

    def _tarea_refresco(self):
        entradas, nodo = pedir_log_al_cluster()
        if entradas is None:
            self.root.after(0, lambda: self._set_estado("No se pudo conectar a ningun nodo del cluster.", "#e74c3c"))
            return
        self.root.after(0, lambda: self._actualizar_tabla(entradas, nodo))

    def _actualizar_tabla(self, entradas, nodo_respondio):
        self._limpiar_tabla()
        self._ultimo_nodo = nodo_respondio or "?"
        self.lbl_nodo.config(text=f"Nodo respondio: {self._ultimo_nodo}")

        if not entradas:
            self._set_estado("El log esta vacio. El cluster aun no ha procesado imagenes.", "#3498db")
            self.lbl_conteo.config(text="Total: 0 registros")
            return

        for i, entrada_str in enumerate(entradas, start=1):
            datos = parsear_entrada(entrada_str)
            tag = datos["clase"] if datos["clase"] in COLORES_CLASE else "DESCONOCIDO"
            imagen = datos.get("imagen", "")
            
            nombre_imagen = os.path.basename(imagen) if imagen else "-"

            item_id = self.tabla.insert(
                "",
                tk.END,
                values=(i, datos["camara"], datos["clase"], datos["fecha"], nombre_imagen),
                tags=(tag,),
            )
            self._imagenes_por_item[item_id] = imagen

        ts = time.strftime("%H:%M:%S")
        self._set_estado(f"Log actualizado a las {ts}. Respondio: {self._ultimo_nodo}", "#2ecc71")
        self.lbl_conteo.config(text=f"Total: {len(entradas)} registros")

    def _limpiar_tabla(self):
        self._imagenes_por_item.clear()
        for item in self.tabla.get_children():
            self.tabla.delete(item)
        if hasattr(self, "lbl_preview"):
            self._preview_img = None
            self.lbl_preview.config(image="", text="Vista previa: seleccione un registro")
            self.lbl_preview_info.config(text="")

    def _mostrar_imagen_seleccionada(self, _event=None):
        seleccion = self.tabla.selection()
        if not seleccion:
            return

        ruta = self._imagenes_por_item.get(seleccion[0], "")
        if not ruta or not os.path.exists(ruta):
            self._preview_img = None
            self.lbl_preview.config(image="", text="Imagen no disponible")
            self.lbl_preview_info.config(text="")
            return

        try:
            img = tk.PhotoImage(file=ruta)
            max_w, max_h = 500, 350
            factor = max(1, math.ceil(max(img.width() / max_w, img.height() / max_h)))
            if factor > 1:
                img = img.subsample(factor, factor)

            self._preview_img = img
            self.lbl_preview.config(image=self._preview_img, text="")
            self.lbl_preview_info.config(
                text=f"Archivo: {os.path.basename(ruta)}\nRuta: {ruta}"
            )
        except tk.TclError:
            self._preview_img = None
            self.lbl_preview.config(image="", text="No se pudo abrir la imagen")
            self.lbl_preview_info.config(text=ruta)

    def _set_estado(self, texto, color="#2ecc71"):
        self.lbl_estado.config(text="  " + texto, fg=color)

    def _toggle_auto_refresh(self):
        self._auto_refresh = not self._auto_refresh
        if self._auto_refresh:
            self.btn_auto.config(text="Detener Auto-Refresh", bg="#c0392b")
            self._set_estado(f"Auto-Refresh cada {INTERVALO_REFRESCO}s activado.", "#f39c12")
            self._ciclo_auto_refresh()
        else:
            self.btn_auto.config(text=f"Auto-Refresh ({INTERVALO_REFRESCO}s)", bg="#27ae60")
            self._set_estado("Auto-Refresh detenido.", "#bdc3c7")

    def _ciclo_auto_refresh(self):
        if self._auto_refresh:
            self._refrescar_manual()
            self.root.after(INTERVALO_REFRESCO * 1000, self._ciclo_auto_refresh)


if __name__ == "__main__":
    root = tk.Tk()
    app = VigilanteApp(root)
    root.mainloop()
