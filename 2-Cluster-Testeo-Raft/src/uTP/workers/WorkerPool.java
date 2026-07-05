package uTP.workers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * DÍA 3 - Tarea 3.2: Thread Pool de Workers de IA
 *
 * Gestiona un pool fijo de 3 hilos (Workers). Cada tarea recibe un vector
 * de características, lo clasifica usando distancia euclidiana contra los
 * centroides del modelo (pesos_ia.json) y guarda el resultado en el log.
 *
 * Cumple la regla: PROHIBIDO Thread.sleep() en el hilo principal.
 * Toda la carga se procesa en los Workers secundarios.
 */
public class WorkerPool {

    // ──────────────────────────────────────────────
    //  Pool de hilos (3 workers = 3 hilos reales)
    // ──────────────────────────────────────────────
    private final ExecutorService executor;

    // ──────────────────────────────────────────────
    //  Log concurrente global de resultados
    //  (el Cliente Vigilante lo leerá para pintar la tabla)
    // ──────────────────────────────────────────────
    public static final ConcurrentLinkedQueue<String> resultLog = new ConcurrentLinkedQueue<>();

    // Contador para nombrar los hilos Worker-IA-1, Worker-IA-2, ...
    private static final java.util.concurrent.atomic.AtomicInteger workerCounter
            = new java.util.concurrent.atomic.AtomicInteger(0);

    // Ruta al archivo de pesos (generado en el Día 1 por Dev B)
    private static final String PESOS_PATH = "../1-Servidor-Entrenamiento/pesos_ia.json";

    public WorkerPool() {
        // Pool fijo de 3 hilos — sin librerías externas
        this.executor = Executors.newFixedThreadPool(3, runnable -> {
            Thread t = new Thread(runnable);
            // Nombre limpio: Worker-IA-1, Worker-IA-2, Worker-IA-3
            t.setName("Worker-IA-" + workerCounter.incrementAndGet());
            t.setDaemon(true); // muere con la JVM
            return t;
        });
        System.out.println("[WorkerPool] 3 Workers de IA iniciados y listos.");
    }

    /**
     * Encola una tarea de clasificación asíncrona.
     *
     * @param nodeId     ID del nodo líder (para logs)
     * @param senderId   ID de la cámara que envió el dato
     * @param payload    Vector de características: "color,aspecto,textura"
     * @param client     Canal del socket de la cámara (para responder CLI_RES).
     *                   Puede ser null si el canal ya fue cerrado (fire-and-forget).
     */
    public void submitTask(String nodeId, String senderId, String payload, SocketChannel client) {
        executor.submit(new WorkerTask(nodeId, senderId, payload, client));
    }

    /** Apaga el pool de forma ordenada al cerrar el nodo. */
    public void shutdown() {
        executor.shutdownNow();
        System.out.println("[WorkerPool] Workers apagados.");
    }

    // ══════════════════════════════════════════════
    //  TAREA INTERNA: Corre en un hilo Worker
    // ══════════════════════════════════════════════
    private static class WorkerTask implements Runnable {

        private final String nodeId;
        private final String senderId;
        private final String payload;
        private final SocketChannel clientChannel;

        WorkerTask(String nodeId, String senderId, String payload, SocketChannel clientChannel) {
            this.nodeId = nodeId;
            this.senderId = senderId;
            this.payload = payload;
            this.clientChannel = clientChannel;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            System.out.println("[" + threadName + "] Procesando clasificación para '" + senderId + "' payload=" + payload);

            try {
                // 1. Parsear el vector recibido: "125,1.25,48"
                double[] vectorConsulta = parseVector(payload);

                // 2. Cargar centroides del modelo entrenado (pesos_ia.json)
                Map<String, double[]> pesos = cargarPesos();

                // 3. Distancia euclidiana → clase más cercana
                String clasificacion = clasificar(vectorConsulta, pesos);

                // 4. Registro con timestamp
                String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        .format(new java.util.Date());
                String entrada = senderId + " | " + clasificacion + " | " + timestamp;

                // 5. Guardar en el log concurrente global
                resultLog.add(entrada);
                System.out.println("[" + threadName + "] ✅ CLASIFICADO: " + entrada);

                // 6. Responder CLI_RES a la cámara (si el socket sigue abierto)
                if (clientChannel != null && clientChannel.isOpen()) {
                    String respuesta = "CLI_RES|" + nodeId + "|0|" + clasificacion + "\n";
                    clientChannel.write(
                        ByteBuffer.wrap(respuesta.getBytes(StandardCharsets.UTF_8))
                    );
                }

            } catch (Exception e) {
                System.err.println("[" + threadName + "] ❌ Error en WorkerTask: " + e.getMessage());
            }
        }

        /** Parsea "125,1.25,48" → double[]{125.0, 1.25, 48.0} */
        private double[] parseVector(String csv) {
            String[] parts = csv.split(",");
            double[] v = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                v[i] = Double.parseDouble(parts[i].trim());
            }
            return v;
        }

        /**
         * Carga pesos_ia.json sin usar librerías externas.
         * Formato esperado:
         * {
         *   "PERRO": [121.66, 1.2, 45.0],
         *   "GATO":  [90.0,  0.9, 70.0],
         *   "CARRO": [200.0, 2.5, 15.0]
         * }
         */
        private Map<String, double[]> cargarPesos() throws IOException {
            Map<String, double[]> pesos = new HashMap<>();

            // Intentar la ruta relativa primero; si falla, buscar en el mismo directorio
            String[] rutas = {PESOS_PATH, "pesos_ia.json", "1-Servidor-Entrenamiento/pesos_ia.json"};
            String contenido = null;

            for (String ruta : rutas) {
                try {
                    contenido = leerArchivo(ruta);
                    break;
                } catch (IOException ignored) {}
            }

            if (contenido == null) {
                // Fallback: usar valores hardcodeados del Día 1 (mismos centroides)
                pesos.put("PERRO", new double[]{121.67, 1.2, 45.0});
                pesos.put("GATO",  new double[]{90.0,   0.9, 70.0});
                pesos.put("CARRO", new double[]{200.0,  2.5, 15.0});
                System.err.println("[WorkerTask] ⚠️ pesos_ia.json no encontrado. Usando centroides hardcodeados del Día 1.");
                return pesos;
            }

            // Parser JSON mínimo sin librerías externas
            // Ejemplo de línea: "PERRO": [121.6, 1.2, 45.0],
            String[] lineas = contenido.split("\n");
            String claseActual = null;
            for (String linea : lineas) {
                linea = linea.trim();

                // Detectar nombre de clase: "PERRO":
                if (linea.startsWith("\"") && linea.contains("\":")) {
                    claseActual = linea.split("\"")[1];
                }

                // Detectar array de valores: [121.6, 1.2, 45.0]
                if (claseActual != null && linea.startsWith("[")) {
                    String sinCorchetes = linea.replace("[", "").replace("]", "").replace(",", " ").trim();
                    String[] vals = sinCorchetes.split("\\s+");
                    double[] arr = new double[vals.length];
                    for (int i = 0; i < vals.length; i++) {
                        arr[i] = Double.parseDouble(vals[i].trim());
                    }
                    pesos.put(claseActual, arr);
                    claseActual = null;
                }
            }
            return pesos;
        }

        private String leerArchivo(String ruta) throws IOException {
            return Files.readString(Paths.get(ruta), StandardCharsets.UTF_8);
        }

        /** Distancia euclidiana entre dos vectores de igual longitud */
        private double distanciaEuclidiana(double[] a, double[] b) {
            double suma = 0;
            int len = Math.min(a.length, b.length);
            for (int i = 0; i < len; i++) {
                suma += Math.pow(a[i] - b[i], 2);
            }
            return Math.sqrt(suma);
        }

        /** Devuelve la clase cuyo centroide está más cerca del vector de consulta */
        private String clasificar(double[] consulta, Map<String, double[]> pesos) {
            String mejorClase = "DESCONOCIDO";
            double menorDistancia = Double.MAX_VALUE;

            for (Map.Entry<String, double[]> entry : pesos.entrySet()) {
                double dist = distanciaEuclidiana(consulta, entry.getValue());
                System.out.println("    distancia a " + entry.getKey() + " = " + String.format("%.2f", dist));
                if (dist < menorDistancia) {
                    menorDistancia = dist;
                    mejorClase = entry.getKey();
                }
            }
            return mejorClase;
        }
    }
}
