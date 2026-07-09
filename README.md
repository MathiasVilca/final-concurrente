# Sistema Distribuido con Consenso Raft e Inteligencia Artificial

Proyecto final de Programacion Concurrente y Distribuida.

El sistema simula un cluster distribuido tolerante a fallos que recibe datos
concurrentes de camaras IP, replica el registro con Raft y clasifica objetos
con un modelo de IA entrenado previamente.

## Arquitectura

```text
CAMARA_N (Python) --CLI_REQ--> LIDER (Java NIO)
                                  |
                    APPEND_ENTRIES|--> FOLLOWER_1
                    APPEND_ENTRIES|--> FOLLOWER_2
                                  |
                              COMMIT
                                  |
                         WorkerPool (3 hilos)
                                  |
                      clasifica con centroides
                                  |
CAMARA_N <--CLI_RES-- LIDER / Worker
```

Protocolo de trama:

```text
TIPO|SENDER|TERM|PAYLOAD\n
```

## Componentes

### 1. Servidor de entrenamiento

Ruta: `1-Servidor-Entrenamiento/`

- `model_trainer.py` calcula centroides para tres clases: `PERRO`, `GATO` y `CARRO`.
- Genera `pesos_ia.json` con los pesos del modelo.
- Usa solo librerias estandar de Python.

### 2. Cluster de testeo con Raft

Ruta: `2-Cluster-Testeo-Raft/`

- Tres nodos Java en puertos `8001`, `8002` y `8003`.
- Servidor TCP no bloqueante con Java NIO y `Selector`.
- Eleccion de lider: `FOLLOWER`, `CANDIDATE`, `LEADER`.
- Heartbeats periodicos.
- Replicacion con `APPEND_ENTRIES` y confirmacion `APPEND_ACK`.
- Commit replicado con `COMMIT_ENTRY`.
- Worker pool de 3 hilos para clasificacion paralela.
- Capturas `.png` en `capturas/`.
- Hosts configurables por variables de entorno para demo local, LAN o WIFI.
- Demo de tolerancia a fallos con `--demo-failover`.

### 3. Emulador de camaras

Ruta: `3-Emulador-Camaras/`

- `video_cameras.py` simula 3 camaras procesando fotogramas de video.
- Puede usar MP4 reales con OpenCV.
- Si OpenCV no esta disponible, usa modo sintetico.
- Puede enviar imagen real codificada en Base64:

```text
color,aspecto,textura##IMG##frame_png_base64
```

- Maneja `REDIRECT` si contacta un nodo que no es lider.
- Soporta `--host` y variables de entorno para LAN/WIFI.

### 4. Cliente vigilante

Ruta: `4-Cliente-Vigilante/`

- `vigilante_app.py` abre una interfaz Tkinter.
- Consulta el log con:

```text
CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG
```

- Muestra camara, clasificacion, fecha/hora y archivo PNG.
- Permite previsualizar la imagen de cada deteccion.
- Soporta variables de entorno para consultar nodos en LAN/WIFI.

## Requisitos

- Java 8 o superior. Probado con Java 21.
- Python 3.7 o superior.
- Opcional: `opencv-python` para procesar MP4 reales.
- Sin internet en ejecucion.
- Sin REST, gRPC, WebSocket, RabbitMQ ni frameworks de mensajeria.

Verificar versiones:

```powershell
java -version
javac -version
python --version
```

## Ejecucion completa paso a paso

Todos los comandos se pueden ejecutar desde PowerShell. No es obligatorio usar
PyCharm ni IntelliJ IDEA.

### Paso 0 - Entrenar el modelo

Desde la raiz del proyecto:

```powershell
cd 1-Servidor-Entrenamiento
python model_trainer.py
cd ..
```

Resultado esperado:

```text
1-Servidor-Entrenamiento/pesos_ia.json
```

### Paso 1 - Compilar Java

Desde la raiz del proyecto:

```powershell
cd 2-Cluster-Testeo-Raft
$files = Get-ChildItem -Recurse -Filter "*.java" -Path "src" | Select-Object -ExpandProperty FullName
javac -encoding UTF-8 -sourcepath src -d out $files
cd ..
```

En Linux, macOS o Git Bash:

```bash
cd 2-Cluster-Testeo-Raft
javac -encoding UTF-8 -sourcepath src -d out $(find src -name "*.java")
cd ..
```

### Paso 2 - Levantar el cluster

Terminal 1:

```powershell
cd 2-Cluster-Testeo-Raft
java -cp out uTP.ClusterLauncher
```

Espera hasta ver un mensaje similar a:

```text
[LIDER] [NODE_X] ME CONVIERTO EN EL LIDER
```

Deja esta terminal abierta.

### Paso 3 - Abrir el cliente vigilante

Terminal 2:

```powershell
cd 4-Cliente-Vigilante
python vigilante_app.py
```

### Paso 4 - Lanzar las camaras

Terminal 3:

```powershell
cd 3-Emulador-Camaras
python video_cameras.py --auto
```

Tambien puedes usar el cliente simple:

```powershell
cd 3-Emulador-Camaras
python camera_client.py --auto
```

## Uso con videos propios

Puedes pasar un MP4 por camara y calibrar su clase esperada:

```powershell
cd 3-Emulador-Camaras
python video_cameras.py --auto `
  --video1 C:\videos\perro.mp4 --tipo1 PERRO `
  --video2 C:\videos\gato.mp4  --tipo2 GATO `
  --video3 C:\videos\auto.mp4  --tipo3 CARRO
```

`--tipoN` indica la clase de calibracion para el modelo de centroides.

Nota: el modelo es academico y simple. Usa centroides con caracteristicas
`[color_promedio, aspecto, textura]`. No es un detector universal tipo YOLO/CNN.

## Pruebas automaticas

Ejecutar desde la raiz del proyecto.

### Prueba end-to-end basica

Levanta el cluster, envia 3 camaras concurrentes, consulta el log y verifica
capturas PNG.

```powershell
python test_e2e.py
```

Resultado esperado:

```text
RESULTADO: 3/3 camaras clasificadas correctamente.
TODO EL SISTEMA FUNCIONA CORRECTAMENTE.
```

### Prueba con videos

Levanta el cluster y ejecuta `video_cameras.py` con los MP4 de `demo_videos/`.

```powershell
python test_video_e2e.py
```

Resultado esperado:

```text
Frames clasificados: 30/30
```

### Prueba de tolerancia a fallos

Levanta el cluster en modo demo, envia una camara, apaga el lider activo,
espera reeleccion y vuelve a enviar otra camara.

```powershell
python test_failover_e2e.py
```

Resultado esperado:

```text
RESULTADO: OK. El cluster respondio antes y despues del failover.
```

### Test unitario del parser de IA

Despues de compilar Java:

```powershell
cd 2-Cluster-Testeo-Raft
java -cp out uTP.workers.WorkerPoolParserTest
cd ..
```

Resultado esperado:

```text
Parser de pesos OK
```

## Demo manual de tolerancia a fallos

Tambien puedes lanzar el cluster con apagado automatico del lider:

```powershell
cd 2-Cluster-Testeo-Raft
java -cp out uTP.ClusterLauncher --demo-failover
```

Por defecto apaga el lider despues de 15 segundos.

Para cambiar el tiempo:

```powershell
java -cp out uTP.ClusterLauncher --demo-failover --failover-delay-ms=6000
```

## Configuracion LAN/WIFI

No es necesario editar el codigo. Usa variables de entorno.

### Caso A: todos los nodos Java corren en una laptop servidor

En la laptop servidor:

```powershell
$env:RAFT_HOST_ALL="192.168.1.50"
$env:RAFT_LISTEN_HOST="0.0.0.0"
cd 2-Cluster-Testeo-Raft
java -cp out uTP.ClusterLauncher
```

En la laptop cliente:

```powershell
$env:CLUSTER_HOST="192.168.1.50"
python 3-Emulador-Camaras/video_cameras.py --auto
python 4-Cliente-Vigilante/vigilante_app.py
```

Tambien puedes pasar el host directamente al emulador:

```powershell
python 3-Emulador-Camaras/video_cameras.py --auto --host 192.168.1.50
```

### Caso B: nodos distribuidos en varias laptops

En cada laptop donde corra Java, configura las IPs reales de los nodos:

```powershell
$env:RAFT_HOST_NODE_1="192.168.1.50"
$env:RAFT_HOST_NODE_2="192.168.1.51"
$env:RAFT_HOST_NODE_3="192.168.1.52"
$env:RAFT_LISTEN_HOST="0.0.0.0"
```

En las laptops cliente:

```powershell
$env:CLUSTER_NODE_1_HOST="192.168.1.50"
$env:CLUSTER_NODE_2_HOST="192.168.1.51"
$env:CLUSTER_NODE_3_HOST="192.168.1.52"
```

Luego ejecuta camaras o vigilante normalmente.

## Variables de entorno disponibles

### Java / Raft

| Variable | Uso | Valor por defecto |
|---|---|---|
| `RAFT_LISTEN_HOST` | IP donde escucha cada nodo Java | `0.0.0.0` |
| `RAFT_HOST_ALL` | Host comun para todos los peers | `127.0.0.1` |
| `RAFT_HOST_NODE_1` | Host de `NODE_1` | valor de `RAFT_HOST_ALL` |
| `RAFT_HOST_NODE_2` | Host de `NODE_2` | valor de `RAFT_HOST_ALL` |
| `RAFT_HOST_NODE_3` | Host de `NODE_3` | valor de `RAFT_HOST_ALL` |

### Python / Clientes

| Variable | Uso | Valor por defecto |
|---|---|---|
| `CLUSTER_HOST` | Host comun del cluster | `127.0.0.1` |
| `CLUSTER_NODE_1_HOST` | Host de `NODE_1` | valor de `CLUSTER_HOST` |
| `CLUSTER_NODE_2_HOST` | Host de `NODE_2` | valor de `CLUSTER_HOST` |
| `CLUSTER_NODE_3_HOST` | Host de `NODE_3` | valor de `CLUSTER_HOST` |

## Flujo de mensajes

```text
CAMARA_N  -> CLI_REQ|CAMARA_N|0|118,1.22,47##IMG##iVBORw0KGgo...
LIDER     -> APPEND_ENTRIES|NODE_1|term|0:CAMARA_N:118,1.22,47##IMG##iVBORw0KGgo...
FOLLOWER  -> APPEND_ACK|NODE_2|term|0
LIDER     -> COMMIT_ENTRY|NODE_1|term|idx::CAMARA_N::timestamp::payload
WORKER    -> CLI_RES|NODE_1|0|PERRO
```

Consulta del vigilante:

```text
CLIENTE_VIGILANTE -> CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG
NODO              -> CLI_RES|NODE_X|0|CAMARA_1|PERRO|fecha|ruta.png;...
```

## Archivos generados en ejecucion

| Ruta | Descripcion |
|---|---|
| `2-Cluster-Testeo-Raft/out/` | Clases Java compiladas |
| `2-Cluster-Testeo-Raft/capturas/` | Imagenes PNG por deteccion |
| `1-Servidor-Entrenamiento/pesos_ia.json` | Centroides del modelo de IA |

## Limpieza opcional

Los tests generan capturas PNG. Puedes borrar manualmente:

```text
2-Cluster-Testeo-Raft/capturas/
```
