"""
DÍA 4 - Cliente Vigilante (Tkinter)
=====================================
Aplicación de escritorio que se conecta al clúster Raft y muestra en
tiempo real una tabla con todas las clasificaciones de las cámaras.

Protocolo de petición: CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG\n
Protocolo de respuesta: CLI_RES|NODE_X|0|<log_serializado>\n
  - El log llega como: "CAMARA_1 | PERRO | 2026-07-04 23:10:05;CAMARA_2 | GATO | ..."

No usa WebSockets ni RabbitMQ. Solo sockets TCP puros.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import socket
import threading
import time

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN DE CONEXIÓN
# ─────────────────────────────────────────────────────────────────────
CLUSTER_HOST  = "127.0.0.1"
CLUSTER_PORTS = [8001, 8002, 8003]
INTERVALO_REFRESCO = 5   # Segundos entre actualizaciones automáticas

# Paleta de colores por clasificación
COLORES_CLASE = {
    "PERRO":        "#2ecc71",   # verde
    "GATO":         "#3498db",   # azul
    "CARRO":        "#e67e22",   # naranja
    "DESCONOCIDO":  "#95a5a6",   # gris
}

# ─────────────────────────────────────────────────────────────────────
#  LÓGICA DE RED (hilo secundario, nunca bloquea la UI)
# ─────────────────────────────────────────────────────────────────────

def pedir_log_al_cluster(timeout=5):
    """
    Envía CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG al clúster y devuelve
    la lista de entradas del log, o None si falla.
    """
    trama = "CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG\n"

    for puerto in CLUSTER_PORTS:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((CLUSTER_HOST, puerto))
                s.sendall(trama.encode("utf-8"))

                # Leer respuesta completa (hasta \n)
                respuesta_raw = b""
                while b"\n" not in respuesta_raw:
                    chunk = s.recv(8192)
                    if not chunk:
                        break
                    respuesta_raw += chunk

                respuesta = respuesta_raw.decode("utf-8", errors="replace").strip()

                # Parsear: CLI_RES|NODE_1|0|<payload>
                partes = respuesta.split("|", 3)
                if len(partes) == 4 and partes[0] == "CLI_RES":
                    payload = partes[3]
                    if payload == "LOG_VACIO":
                        return [], partes[1]
                    # Dividir entradas por ";"
                    entradas = [e.strip() for e in payload.split(";") if e.strip()]
                    return entradas, partes[1]

        except Exception:
            continue

    return None, None   # Todos los nodos fallaron


def parsear_entrada(entrada_str):
    """
    Convierte "CAMARA_1 | PERRO | 2026-07-04 23:10:05" en un dict.
    """
    partes = [p.strip() for p in entrada_str.split("|")]
    if len(partes) >= 3:
        return {"camara": partes[0], "clase": partes[1], "fecha": partes[2]}
    elif len(partes) == 2:
        return {"camara": partes[0], "clase": partes[1], "fecha": "—"}
    else:
        return {"camara": entrada_str, "clase": "?", "fecha": "—"}


# ─────────────────────────────────────────────────────────────────────
#  INTERFAZ GRÁFICA TKINTER
# ─────────────────────────────────────────────────────────────────────

class VigilanteApp:
    def __init__(self, root):
        self.root = root
        self.root.title("🔭 Cliente Vigilante — Clúster Raft + IA")
        self.root.geometry("860x580")
        self.root.configure(bg="#1a1a2e")
        self.root.resizable(True, True)

        self._auto_refresh = False
        self._ultimo_nodo  = "—"
        self._construir_ui()

    # ── Construcción de la UI ────────────────────────────────────────

    def _construir_ui(self):
        # ── Cabecera ─────────────────────────────────────────────────
        cabecera = tk.Frame(self.root, bg="#16213e", pady=10)
        cabecera.pack(fill=tk.X)

        tk.Label(
            cabecera,
            text="🔭  Cliente Vigilante — Sistema Distribuido con Raft + IA",
            font=("Segoe UI", 14, "bold"),
            fg="#e0e0e0",
            bg="#16213e",
        ).pack(side=tk.LEFT, padx=20)

        # Estado del nodo conectado
        self.lbl_nodo = tk.Label(
            cabecera,
            text="Nodo: —",
            font=("Segoe UI", 10),
            fg="#f39c12",
            bg="#16213e",
        )
        self.lbl_nodo.pack(side=tk.RIGHT, padx=20)

        # ── Barra de controles ────────────────────────────────────────
        barra = tk.Frame(self.root, bg="#0f3460", pady=8)
        barra.pack(fill=tk.X)

        tk.Label(
            barra,
            text=f"  Host: {CLUSTER_HOST}   Puertos: {CLUSTER_PORTS}",
            font=("Segoe UI", 9),
            fg="#bdc3c7",
            bg="#0f3460",
        ).pack(side=tk.LEFT, padx=10)

        # Botón Refrescar
        self.btn_refrescar = tk.Button(
            barra,
            text="🔄  Refrescar Ahora",
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

        # Botón Auto-Refresh
        self.btn_auto = tk.Button(
            barra,
            text=f"▶  Auto-Refresh ({INTERVALO_REFRESCO}s)",
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

        # Botón Limpiar
        tk.Button(
            barra,
            text="🗑  Limpiar",
            command=self._limpiar_tabla,
            bg="#7f8c8d",
            fg="white",
            font=("Segoe UI", 9),
            relief=tk.FLAT,
            padx=8,
            pady=4,
            cursor="hand2",
        ).pack(side=tk.RIGHT, padx=4)

        # ── Tabla de resultados ───────────────────────────────────────
        contenedor = tk.Frame(self.root, bg="#1a1a2e")
        contenedor.pack(fill=tk.BOTH, expand=True, padx=16, pady=(12, 0))

        columnas = ("#", "Cámara", "Clasificación IA", "Fecha / Hora")
        self.tabla = ttk.Treeview(
            contenedor,
            columns=columnas,
            show="headings",
            height=18,
        )

        # Estilos de la tabla
        estilo = ttk.Style()
        estilo.theme_use("clam")
        estilo.configure(
            "Treeview",
            background="#16213e",
            foreground="#ecf0f1",
            rowheight=28,
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

        # Configurar columnas
        anchos = {"#": 50, "Cámara": 140, "Clasificación IA": 180, "Fecha / Hora": 200}
        for col in columnas:
            self.tabla.heading(col, text=col)
            self.tabla.column(col, width=anchos.get(col, 150), anchor=tk.CENTER)

        # Scrollbar
        scroll_y = ttk.Scrollbar(contenedor, orient=tk.VERTICAL, command=self.tabla.yview)
        self.tabla.configure(yscrollcommand=scroll_y.set)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.tabla.pack(fill=tk.BOTH, expand=True)

        # Tags de color por clase
        for clase, color in COLORES_CLASE.items():
            self.tabla.tag_configure(clase, foreground=color)

        # ── Barra de estado inferior ──────────────────────────────────
        self.barra_estado = tk.Frame(self.root, bg="#16213e", pady=6)
        self.barra_estado.pack(fill=tk.X, side=tk.BOTTOM)

        self.lbl_estado = tk.Label(
            self.barra_estado,
            text="  Listo. Presiona 'Refrescar Ahora' para consultar el clúster.",
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

    # ── Acciones ─────────────────────────────────────────────────────

    def _refrescar_manual(self):
        """Lanza el refresco en un hilo separado para no bloquear la UI."""
        self._set_estado("⏳ Consultando el clúster...", "#f39c12")
        threading.Thread(target=self._tarea_refresco, daemon=True).start()

    def _tarea_refresco(self):
        """Corre en hilo secundario. Consulta el clúster y actualiza la UI."""
        entradas, nodo = pedir_log_al_cluster()

        if entradas is None:
            self.root.after(0, lambda: self._set_estado(
                "❌ No se pudo conectar a ningún nodo del clúster.", "#e74c3c"
            ))
            return

        self.root.after(0, lambda: self._actualizar_tabla(entradas, nodo))

    def _actualizar_tabla(self, entradas, nodo_respondio):
        """Actualiza la tabla con los datos recibidos (corre en hilo de UI)."""
        self._limpiar_tabla()

        self._ultimo_nodo = nodo_respondio or "?"
        self.lbl_nodo.config(text=f"Nodo respondió: {self._ultimo_nodo}")

        if not entradas:
            self._set_estado("ℹ️  El log está vacío. El clúster aún no ha procesado imágenes.", "#3498db")
            self.lbl_conteo.config(text="Total: 0 registros")
            return

        for i, entrada_str in enumerate(entradas, start=1):
            datos = parsear_entrada(entrada_str)
            tag   = datos["clase"] if datos["clase"] in COLORES_CLASE else "DESCONOCIDO"
            self.tabla.insert(
                "",
                tk.END,
                values=(i, datos["camara"], datos["clase"], datos["fecha"]),
                tags=(tag,),
            )

        ts = time.strftime("%H:%M:%S")
        self._set_estado(f"✅ Log actualizado a las {ts}. Respondió: {self._ultimo_nodo}", "#2ecc71")
        self.lbl_conteo.config(text=f"Total: {len(entradas)} registros")

    def _limpiar_tabla(self):
        for item in self.tabla.get_children():
            self.tabla.delete(item)

    def _set_estado(self, texto, color="#2ecc71"):
        self.lbl_estado.config(text=f"  {texto}", fg=color)

    def _toggle_auto_refresh(self):
        self._auto_refresh = not self._auto_refresh
        if self._auto_refresh:
            self.btn_auto.config(text=f"⏹  Detener Auto-Refresh", bg="#c0392b")
            self._set_estado(f"🔄 Auto-Refresh cada {INTERVALO_REFRESCO}s activado.", "#f39c12")
            self._ciclo_auto_refresh()
        else:
            self.btn_auto.config(text=f"▶  Auto-Refresh ({INTERVALO_REFRESCO}s)", bg="#27ae60")
            self._set_estado("⏸  Auto-Refresh detenido.", "#bdc3c7")

    def _ciclo_auto_refresh(self):
        """Ciclo periódico que respeta si el auto-refresh está activo."""
        if self._auto_refresh:
            self._refrescar_manual()
            self.root.after(INTERVALO_REFRESCO * 1000, self._ciclo_auto_refresh)


# ─────────────────────────────────────────────────────────────────────
#  PUNTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    root = tk.Tk()
    app  = VigilanteApp(root)

    # Icono en la barra de tareas (si hay Tkinter con soporte de imagen)
    try:
        root.iconbitmap(default="")
    except Exception:
        pass

    root.mainloop()
