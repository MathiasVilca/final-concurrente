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

## Parte 2: Guía de Trabajo para Dev B (Días 3 y 4)

**Estado:** Pendiente

Hola, Dev B. La red es indestructible y los nodos ya saben elegir un líder. Tu misión ahora es darle utilidad al clúster: inyectar el tráfico de las cámaras, replicar los datos en los seguidores y usar la IA para clasificarlos.

## Día 3: Concurrencia, Replicación de Logs e Inferencia IA

### Tarea 3.1: Replicación del Registro (Raft Log Replication)

Actualmente, el `NioServer` del líder recibe la petición de la cámara y responde con un `ACK`. Según Raft, un líder nunca procesa la petición de inmediato.

Pendientes:

- En `RaftController.java`, agregar un nuevo tipo de mensaje: `APPEND_ENTRIES`.
- Cuando el líder recibe un `CLI_REQ` de una cámara, debe reenviar ese payload envuelto en un `APPEND_ENTRIES` a los otros dos seguidores.
- Los seguidores deben guardar el dato en una lista en memoria y responder un `APPEND_ACK`.
- Solo cuando el líder recibe la mayoría de `ACKs` (`2 de 3`), da el dato por **committed** (comprometido) y pasa al paso 3.2.

### Tarea 3.2: Thread Pool (Workers de IA)

La rúbrica exige el uso de hilos para procesar la carga.

Pendientes:

- Crear un `WorkerPool`, usando `ExecutorService` o arreglos de `Thread` puros.
- Cuando un dato es **committed**, el líder se lo pasa a un hilo worker.
- El hilo worker lee el vector recibido, por ejemplo `[125, 1.25, 48]`.
- El hilo worker carga `pesos_ia.json`.
- El hilo worker calcula la distancia euclidiana más corta para clasificar si el dato corresponde a `PERRO`, `GATO` o `CARRO`.
- El hilo worker envía el resultado final de vuelta a la cámara correspondiente (`CLI_RES`).

### Pruebas a cumplir en el Día 3

Para certificar que el código funciona, entrar a la carpeta `3-Emulador-Camaras` y ejecutar:

```bash
python camaras.py
```

#### Criterio de éxito

Se debe ver en la consola del clúster cómo el líder recibe 9 peticiones de golpe (`3 cámaras x 3 fotos`), las encola, las replica en los seguidores sin que los hilos choquen entre sí, las clasifica correctamente con la IA y devuelve el resultado a las cámaras.

## Día 4: Despliegue, Monitoreo y Documentación

### Tarea 4.1: Interfaz / Monitor de Nodos

La rúbrica exige poder ingresar cada cliente, los registros uno por uno y visualizarlos en los monitores de los workers.

Pendientes:

- Crear un pequeño script en Python (`monitor.py`) o una UI en la terminal de Java que se conecte a cada nodo.
- El monitor debe imprimir la lista de datos **committed** de cada nodo para demostrar que los 3 tienen exactamente la misma información (consistencia eventual).

### Tarea 4.2: Despliegue en Red Real (LAN/WiFi)

Hasta ahora se ha probado en `127.0.0.1`.

Para el examen:

- Dev A y Dev B deben estar en la misma red WiFi.
- Deben modificar `RaftConfig.java` para poner las IPs reales de sus laptops en lugar de `localhost`.
- Deben levantar el `NioServer` cada uno en su máquina.

### Pruebas a cumplir en el Día 4 (Simulacro de Presentación)

#### Criterio de éxito 1

Ejecutar `camaras.py` desde la Laptop A apuntando a la IP de la Laptop B, que debe ser el líder. Las inferencias deben viajar por WiFi y regresar.

#### Criterio de éxito 2

Apagar el WiFi de la laptop del líder repentinamente. Las laptops restantes deben elegir un nuevo líder en menos de 3 segundos sin que el programa se cierre.

### Tarea 4.3: Artefactos Finales para Univirtual

Generar los documentos obligatorios:

- Diagrama de arquitectura del clúster: nodos, clientes y workers.
- Diagrama de secuencia del protocolo: `CLI_REQ -> APPEND_ENTRIES -> ACK -> Inferencia IA -> CLI_RES`.
- Subir solo el código fuente y los PDFs, tal como especifica el examen final.

## Cómo ejecutar el proyecto en tu entorno local

Si acabas de clonar este repositorio, sigue estos pasos para comprobar que la base funciona.

### 1. Abrir el proyecto

Abre el proyecto en IntelliJ IDEA.

### 2. Ejecutar el clúster

Ve a `uTP/ClusterLauncher.java` y ejecútalo.

Verás los 3 nodos encenderse y elegir a un líder automáticamente. La consola se quedará en silencio porque los heartbeats están ocultos para no molestar.

### 3. Ejecutar el entorno de la cámara

Abre una terminal y ejecuta:

```bash
cd 3-Emulador-Camaras
python camaras.py
```

### 4. Probar la red manualmente

Si quieres jugar con la red manualmente, ejecuta `uTP/TestClient.java` en IntelliJ para enviar cadenas crudas al clúster y probar la respuesta a fallos.

## Cierre

Éxitos cerrando los Días 3 y 4.
