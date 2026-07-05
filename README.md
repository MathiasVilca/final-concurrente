# Sistema Distribuido con Consenso Raft e Inteligencia Artificial

Este repositorio contiene la implementación final para el examen de **Programación Concurrente y Distribuida**.

El sistema simula un clúster distribuido tolerante a fallos que recibe flujos de datos concurrentes de múltiples cámaras IP y los procesa usando un modelo de Inteligencia Artificial basado en distancias matemáticas y centroides.

## Parte 1: Lo que hemos logrado (Días 1 y 2)

**Estado:** Completado

En esta primera mitad del proyecto se construyó toda la infraestructura de red subyacente y la máquina de estados del clúster desde cero, cumpliendo la regla estricta de no usar frameworks ni librerías externas de comunicación:

- Cero REST.
- Cero gRPC.
- Cero RabbitMQ.

### 1. Arquitectura de Red Asíncrona (Java NIO)

Para soportar múltiples conexiones concurrentes sin colapsar por bloqueos de I/O, se implementó el paquete `uTP.network`.

#### `NioServer.java`

Utiliza `Selector` y `ServerSocketChannel` en modo no bloqueante. Un solo hilo es capaz de gestionar múltiples conexiones entrantes.

#### `PeerClient.java`

Implementa una arquitectura **Fire-and-Forget** (disparar y olvidar).

Para la comunicación entre nodos, se abre un socket, se envían los bytes y se cierra inmediatamente. Esto previene interbloqueos (*deadlocks*) y tuberías rotas si un nodo se cae inesperadamente.

#### Protocolo custom (`Message.java`)

Como no se usa JSON en Java, se creó un protocolo delimitado por `|` y `\n`.

Ejemplo:

```text
TIPO|SENDER|TERM|PAYLOAD\n
```

También se implementó un analizador (`deserialize`) blindado contra espacios y caracteres corruptos.

### 2. Algoritmo de Consenso Raft (Elección de Líder)

Ubicado en el paquete `uTP.raft`, el clúster es capaz de organizarse por sí mismo sin intervención humana.

#### `RaftController.java`

Es el cerebro de cada nodo. Administra los 3 estados fundamentales:

- `FOLLOWER`
- `CANDIDATE`
- `LEADER`

#### Temporizadores aleatorios

Cada seguidor espera entre `1500 ms` y `3000 ms`. Si no recibe noticias, inicia una elección.

#### Heartbeats (latidos)

El nodo que gana la elección asume el rol de líder y envía latidos a los seguidores cada `800 ms` para mantener el liderazgo.

#### Tolerancia a fallos certificada

Si el líder actual muere o se desconecta, los seguidores detectan el silencio en la red y automáticamente inician una nueva elección, elevando el `TERM` (mandato) para elegir un nuevo líder.

### 3. Modelo de IA Distribuido (Python Puro)

En la carpeta `1-Servidor-Entrenamiento`:

#### `model_trainer.py`

Simula el entrenamiento de Machine Learning calculando los centroides de un dataset:

- `PERRO`
- `GATO`
- `CARRO`

Exporta los pesos a un archivo `pesos_ia.json` usando solo librerías estándar.

## Parte 2: Días 3 y 4 — COMPLETADO ✅

### Nuevos componentes implementados

#### `uTP/workers/WorkerPool.java`
- `ExecutorService` con **pool fijo de 3 hilos** (`Worker-IA-*`)
- Cada `WorkerTask` parsea el vector `[color, aspecto, textura]`, carga `pesos_ia.json` y calcula la **distancia euclidiana** al centroide más cercano
- Guarda el resultado en `WorkerPool.resultLog` (`ConcurrentLinkedQueue<String>`) — log global concurrente
- Responde `CLI_RES|NODO|0|CLASIFICACION` a la cámara cuando termina

#### `uTP/raft/RaftController.java` — Día 3 (Replicación de Log)
- **`APPEND_ENTRIES`**: el líder envía el payload a todos los followers para replicar
- **`APPEND_ACK`**: el follower confirma. Al recibir la mayoría (≥ 1 follower = 2/3 nodos), el dato queda **committed**
- **`handleExternalRequest()`**: punto de entrada para peticiones externas del NioServer
- **`getLogAsString()`**: serializa el log para el Cliente Vigilante

#### `uTP/network/NioServer.java` — Día 3 (Integración)
- `CLI_REQ` con payload `GET_LOG` → responde el log completo al Cliente Vigilante
- `CLI_REQ` normal → si es líder, llama a `RaftController.handleExternalRequest()`. Si no, envía `REDIRECT`
- Buffer aumentado a **64KB** para soportar payloads grandes

#### `3-Emulador-Camaras/camera_client.py` — Día 3
- **3 hilos** Python simultáneos (1 por cámara)
- Cada cámara envía 3 vectores al clúster con 3s de pausa
- Maneja timeout, REDIRECT y reconexión

#### `4-Cliente-Vigilante/vigilante_app.py` — Día 4
- UI Tkinter dark-mode con tabla de resultados
- Columnas: `Cámara | Clasificación IA | Fecha/Hora`
- Botón **Refrescar Ahora** y **Auto-Refresh** configurable (cada 5s)
- Colores por clasificación: PERRO=verde, GATO=azul, CARRO=naranja

---

## Cómo ejecutar el sistema completo

### 1. Compilar el proyecto Java (una sola vez)

```bash
cd 2-Cluster-Testeo-Raft
javac -encoding UTF-8 -sourcepath src -d out $(find src -name "*.java")
# En PowerShell:
# $files = Get-ChildItem -Recurse -Filter "*.java" -Path "src" | Select-Object -ExpandProperty FullName
# javac -encoding UTF-8 -sourcepath src -d out $files
```

### 2. Levantar el clúster (Terminal 1)

En IntelliJ: ejecutar `uTP/ClusterLauncher.java`

O desde consola:
```bash
cd 2-Cluster-Testeo-Raft
java -cp out uTP.ClusterLauncher
```

Verás los 3 nodos arrancar y elegir un LÍDER automáticamente.

### 3. Lanzar las cámaras (Terminal 2)

```bash
cd 3-Emulador-Camaras
python camera_client.py
```

Presiona ENTER cuando lo pida. Las 3 cámaras enviarán sus vectores concurrentemente.

### 4. Abrir el Cliente Vigilante (Terminal 3)

```bash
cd 4-Cliente-Vigilante
python vigilante_app.py
```

Presiona **Refrescar Ahora** para ver la tabla de clasificaciones.

### 5. Demo de Tolerancia a Fallos (para el profesor)

En `ClusterLauncher.java`, descomenta el bloque `/* */` del final.  
El Nodo 1 morirá a los 15s y los otros dos elegirán un nuevo líder en < 3s.

---

## Flujo completo del mensaje (Protocolo)

```
CAMARA_N  →[CLI_REQ|CAMARA_N|0|125,1.25,48\n]→  LEADER:8001
LEADER    →[APPEND_ENTRIES|NODE_1|term|0:CAMARA_N:125,1.25,48\n]→  NODE_2, NODE_3
NODE_2    →[APPEND_ACK|NODE_2|term|0\n]→  LEADER
LEADER    → commit → WorkerPool (hilo Worker-IA)
Worker    → distancia euclidiana → "PERRO"
Worker    →[CLI_RES|NODE_1|0|PERRO\n]→  CAMARA_N

CLIENTE_VIGILANTE  →[CLI_REQ|CLIENTE_VIGILANTE|0|GET_LOG\n]→  ANY_NODE
ANY_NODE           →[CLI_RES|NODE_X|0|CAMARA_1 | PERRO | 2026-07-04 23:10:05;...\n]
```

