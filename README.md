# Sistema Distribuido con Consenso Raft e Inteligencia Artificial

Proyecto final de Programacion Concurrente y Distribuida.
El sistema simula un cluster distribuido tolerante a fallos que recibe datos
concurrentes de camaras IP, replica el registro con Raft y clasifica objetos
con un modelo de IA entrenado previamente.

## Arquitectura

```
CAMARA_N (Python) ──CLI_REQ──► LIDER (Java NIO)
                                 │
                    APPEND_ENTRIES│◄─ FOLLOWER_1
                    APPEND_ENTRIES│◄─ FOLLOWER_2
                                 │
                         COMMIT  ▼
                          WorkerPool (3 hilos)
                                 │
                    clasifica con centroides
                                 │
                         CLI_RES ▼
                     ◄────────── CAMARA_N
```

Protocolo de trama: `TIPO|SENDER|TERM|PAYLOAD\n`

## Componentes

### 1. Servidor de entrenamiento  `1-Servidor-Entrenamiento/`

- `model_trainer.py` calcula centroides para tres clases: `PERRO`, `GATO` y `CARRO`.
- Genera `pesos_ia.json` con los centroides.
- Solo usa librerias estandar de Python.

```powershell
cd 1-Servidor-Entrenamiento
python model_trainer.py
```

### 2. Cluster con Raft  `2-Cluster-Testeo-Raft/`

Tres nodos Java en puertos `8001`, `8002` y `8003`.

Funciones:
- Servidor TCP no bloqueante con Java NIO y `Selector`.
- Eleccion de lider: estados `FOLLOWER`, `CANDIDATE` y `LEADER`.
- Heartbeats periodicos cada 800 ms.
- Replicacion con `APPEND_ENTRIES` / confirmacion `APPEND_ACK`.
- Worker pool de 3 hilos (`ExecutorService`) para clasificacion paralela.
- Capturas `.png` por deteccion en `capturas/`: si la camara envia un frame real
  se guarda ese frame; si no hay video/OpenCV se genera una imagen sintetica.
- Log replicado consultable con `GET_LOG`.

### 3. Emulador de camaras con Video `3-Emulador-Camaras/`

- `video_cameras.py` simula cámaras procesando fotogramas de video.
- Soporta 2 modos:
  - **Video real:** Lee MP4 usando OpenCV y extrae características de cada frame.
  - **Modo sintético:** Si no hay OpenCV, genera fotogramas matemáticamente.
- Soporta videos personalizados por camara con `--video1`, `--video2`, `--video3`
  y calibracion de clase con `--tipo1`, `--tipo2`, `--tipo3`.
- `crear_videos_demo.py` genera 3 videos MP4 de prueba usando OpenCV.
- Cada cámara envía los fotogramas en tiempo real al clúster Java.
- El payload puede incluir imagen real: `color,aspecto,textura##IMG##frame_png_base64`.
- Maneja reconexión `REDIRECT` si falla el líder Raft.

### 4. Cliente vigilante  `4-Cliente-Vigilante/`

- `vigilante_app.py` interfaz Tkinter rediseñada (Dashboard).
- Monitor Raft superior en tiempo real que hace *pings* para saber qué nodos están UP/DOWN.
- Consulta el log con `CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG`.
- Panel dividido (Split-View): Tabla de registros a la izquierda y foto en tamaño grande a la derecha.
- Bounding Boxes: Las imágenes PNG ahora incluyen el marco amarillo y el texto de detección dibujados de forma nativa por el servidor Java.

## Requisitos

- Java 8 o superior (probado con Java 21).
- Python 3.7 o superior.
- Sin internet. Sin librerias externas (no REST, no gRPC, no WebSocket, no RabbitMQ).

## Ejecucion completa paso a paso

### Paso 0 — Entrenar el modelo (solo una vez)

```powershell
cd 1-Servidor-Entrenamiento
python model_trainer.py
```

Genera `pesos_ia.json` con los centroides de PERRO, GATO y CARRO.

### Paso 1 — Compilar Java

**PowerShell:**
```powershell
cd 2-Cluster-Testeo-Raft
$files = Get-ChildItem -Recurse -Filter "*.java" -Path "src" | Select-Object -ExpandProperty FullName
javac -encoding UTF-8 -sourcepath src -d out $files
```

**Linux / macOS / Git Bash:**
```bash
cd 2-Cluster-Testeo-Raft
javac -encoding UTF-8 -sourcepath src -d out $(find src -name "*.java")
```

### Paso 2 — Levantar el cluster (Terminal 1)

```powershell
cd 2-Cluster-Testeo-Raft
java -cp out uTP.ClusterLauncher
```

Espera hasta ver el mensaje `[LIDER] [NODE_X] ME CONVIERTO EN EL LIDER`.

### Paso 3 — Abrir el Cliente Vigilante (Terminal 2)

```powershell
cd 4-Cliente-Vigilante
python vigilante_app.py
```

El Dashboard mostrará el estado de los nodos Raft (Verde = UP, Rojo = DOWN).
La tabla se auto-actualizará.

### Paso 4 — Lanzar las Cámaras (Terminal 3)

**Opción A: Ejecutar el test completo automatizado con videos reales:**
(Asegúrate de haber instalado `pip install opencv-python`)
```powershell
python test_video_e2e.py
```

**Opción B: Lanzar cámaras manualmente con videos propios:**
```powershell
cd 3-Emulador-Camaras
python video_cameras.py
```

`--tipoN` indica la clase esperada/calibración de la cámara para el modelo de centroides. El sistema acepta cualquier MP4 local, extrae frames y el servidor Java los procesará, dibujará el **Bounding Box amarillo**, y guardará el PNG final en `capturas/`.

*Nota: Si no tienes OpenCV, el sistema funcionará automáticamente en "Modo Sintético" generando fotogramas por matemática, por lo que nunca fallará la demostración.*

> Importante: este proyecto usa un modelo academico simple de centroides con
> caracteristicas `[color_promedio, aspecto, textura]`. Por eso puede procesar
> cualquier MP4, pero no es un detector visual universal tipo YOLO/CNN. Para la
> exposicion se recomienda usar videos donde cada camara este calibrada como
> PERRO, GATO o CARRO.

> exposición se recomienda usar los videos en `demo_videos` donde cada cámara este calibrada como PERRO, GATO o CARRO.
Haz clic en una fila para previsualizar la imagen `.png`.

## Flujo de mensajes

```
CAMARA_N  ->  CLI_REQ|CAMARA_N|0|118,1.22,47##IMG##iVBORw0KGgo...
LIDER     ->  APPEND_ENTRIES|NODE_1|term|0:CAMARA_N:118,1.22,47##IMG##iVBORw0KGgo...
FOLLOWER  ->  APPEND_ACK|NODE_2|term|0
LIDER     ->  commit -> WorkerPool -> separa vector/imagen -> clasifica -> guarda .png
WORKER    ->  CLI_RES|NODE_1|0|PERRO
```

```
CLIENTE_VIGILANTE  ->  CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG
NODO               ->  CLI_RES|NODE_X|0|CAMARA_1 | PERRO | 2026-07-05 02:36:11 | ruta.png;...
```

## Prueba automatica end-to-end

Ejecutar desde la raiz del proyecto:

```powershell
python test_e2e.py
```

Resultado esperado:

```
[OK] CAMARA_1: CLI_RES|NODE_1|0|PERRO
[OK] CAMARA_2: CLI_RES|NODE_1|0|GATO
[OK] CAMARA_3: CLI_RES|NODE_1|0|CARRO
TODO EL SISTEMA FUNCIONA CORRECTAMENTE.
```

## Test unitario del parser de IA

```powershell
cd 2-Cluster-Testeo-Raft
java -cp out uTP.workers.WorkerPoolParserTest
```

Resultado esperado: `Parser de pesos OK`

## Tolerancia a fallos — Demo para el profesor

Descomenta el bloque al final de `ClusterLauncher.java` antes de presentar:

```java
// Esto mata NODE_1 a los 15s para demostrar re-eleccion
try {
    Thread.sleep(15000);
    System.out.println("[DEMO] MATANDO NODO 1 PARA PROBAR RAFT");
    nodo1.shutdown();
} catch (InterruptedException e) { e.printStackTrace(); }
```

Los dos nodos restantes detectan la ausencia de heartbeats y eligen un nuevo
lider en menos de 3 segundos. El sistema sigue respondiendo a las camaras.

## Despliegue en red LAN (2 laptops)

En `2-Cluster-Testeo-Raft/src/uTP/raft/RaftConfig.java` cambia `"127.0.0.1"`
por las IPs reales de las laptops y redistribuye los nodos.

## Archivos generados en ejecucion

| Ruta | Descripcion |
|------|-------------|
| `2-Cluster-Testeo-Raft/out/` | Clases Java compiladas |
| `2-Cluster-Testeo-Raft/capturas/` | Imagenes `.png` por deteccion |
| `1-Servidor-Entrenamiento/pesos_ia.json` | Centroides del modelo de IA |