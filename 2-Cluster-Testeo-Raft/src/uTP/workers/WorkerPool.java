package uTP.workers;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.BasicStroke;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.imageio.ImageIO;

/**
 * Pool concurrente de workers de IA.
 *
 * Cada tarea clasifica un vector de caracteristicas usando el modelo entrenado
 * en pesos_ia.json, guarda una captura PNG y registra la deteccion para el
 * cliente vigilante.
 */
public class WorkerPool {

    private final ExecutorService executor;

    /** Log consultado por el Cliente Vigilante. Formato: camara|clase|fecha|imagen */
    public static final ConcurrentLinkedQueue<String> resultLog = new ConcurrentLinkedQueue<>();

    /** Evita repetir registros/capturas cuando llegan varios frames de la misma camara. */
    private static final java.util.Set<String> deteccionesRegistradas =
            java.util.Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /** Ruta consensuada de la primera imagen para cada camara/video. */
    private static final ConcurrentHashMap<String, String> rutasPorDeteccion = new ConcurrentHashMap<>();

    private static final AtomicInteger workerCounter = new AtomicInteger(0);
    private static final String PESOS_PATH = "../1-Servidor-Entrenamiento/pesos_ia.json";
    private static final String IMG_MARKER = "##IMG##";

    public WorkerPool() {
        this.executor = Executors.newFixedThreadPool(3, runnable -> {
            Thread t = new Thread(runnable);
            t.setName("Worker-IA-" + workerCounter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        System.out.println("[WorkerPool] 3 Workers de IA iniciados y listos.");
    }

    /** Firma usada por RaftController al comprometer una entrada del log. */
    public void submitTask(
            String nodeId,
            int idx,
            String senderId,
            String payload,
            String timestamp,
            boolean guardarImagen,
            SocketChannel client
    ) {
        executor.submit(new WorkerTask(nodeId, idx, senderId, payload, timestamp, guardarImagen, client));
    }

    /** Compatibilidad con versiones anteriores del cliente de camaras. */
    public void submitTask(String nodeId, String senderId, String payload, SocketChannel client) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        submitTask(nodeId, -1, senderId, payload, timestamp, true, client);
    }

    public synchronized String getResultLogAsString() {
        if (resultLog.isEmpty()) {
            return "LOG_VACIO";
        }
        StringBuilder sb = new StringBuilder();
        for (String entrada : resultLog) {
            if (entrada != null && !entrada.trim().isEmpty()) {
                if (sb.length() > 0) sb.append(";");
                sb.append(entrada);
            }
        }
        return sb.length() == 0 ? "LOG_VACIO" : sb.toString();
    }

    public void shutdown() {
        executor.shutdownNow();
        System.out.println("[WorkerPool] Workers apagados.");
    }

    /** Parser JSON minimo sin dependencias externas para pesos_ia.json. */
    public static Map<String, double[]> parsePesosFromJsonContent(String contenido) {
        Map<String, double[]> pesos = new HashMap<>();
        if (contenido == null) {
            return pesos;
        }

        Pattern entryPattern = Pattern.compile("\\\"([^\\\"]+)\\\"\\s*:\\s*\\[([^\\]]*)\\]", Pattern.MULTILINE | Pattern.DOTALL);
        Matcher matcher = entryPattern.matcher(contenido);
        while (matcher.find()) {
            String clase = matcher.group(1).trim().toUpperCase();
            String[] valores = matcher.group(2).split(",");
            double[] vector = new double[valores.length];
            for (int i = 0; i < valores.length; i++) {
                vector[i] = Double.parseDouble(valores[i].trim());
            }
            pesos.put(clase, vector);
        }
        return pesos;
    }

    private static class WorkerTask implements Runnable {
        private final String nodeId;
        private final int idx;
        private final String senderId;
        private final String payload;
        private final String timestamp;
        private final boolean guardarImagen;
        private final SocketChannel clientChannel;

        WorkerTask(
                String nodeId,
                int idx,
                String senderId,
                String payload,
                String timestamp,
                boolean guardarImagen,
                SocketChannel clientChannel
        ) {
            this.nodeId = nodeId;
            this.idx = idx;
            this.senderId = senderId;
            this.payload = payload;
            this.timestamp = timestamp;
            this.guardarImagen = guardarImagen;
            this.clientChannel = clientChannel;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            try {
                System.out.println("[" + threadName + "] Procesando IA para " + senderId + " payload=" + payload);

                String vectorPayload = extraerVectorPayload(payload);
                String imagenBase64 = extraerImagenBase64(payload);

                double[] vectorConsulta = parseVector(vectorPayload);
                Map<String, double[]> pesos = cargarPesos();
                String clasificacion = clasificar(vectorConsulta, pesos);

                String rutaImagen = construirRutaImagen(senderId, clasificacion, timestamp, idx);
                // Clave por entrada Raft: registra cada frame comprometido una sola vez
                // entre los nodos del mismo JVM, pero permite actividad continua de video.
                String claveDeteccion = idx >= 0 ? "IDX_" + idx : senderId + "_" + timestamp;
                String rutaPrincipal = rutasPorDeteccion.putIfAbsent(claveDeteccion, rutaImagen);
                boolean primeraDeteccion = (rutaPrincipal == null);
                if (primeraDeteccion) {
                    rutaPrincipal = rutaImagen;
                }

                // Si Python envio un frame real en Base64, se guarda ese PNG real.
                // Si no vino imagen, se genera una captura sintetica de respaldo.
                if (guardarImagen && (primeraDeteccion || !Files.exists(Paths.get(rutaPrincipal)))) {
                    guardarCapturaTolerante(Paths.get(rutaPrincipal), imagenBase64, senderId, clasificacion, vectorConsulta, timestamp);
                }

                // Registrar cada entrada Raft una sola vez. Asi el vigilante ve un
                // historial continuo sin duplicados por leader/followers.
                if (deteccionesRegistradas.add(claveDeteccion)) {
                    String entrada = senderId + "|" + clasificacion + "|" + timestamp + "|" + rutaPrincipal;
                    resultLog.add(entrada);
                    System.out.println("[" + threadName + "] OK REGISTRO FRAME: " + entrada);
                } else {
                    System.out.println("[" + threadName + "] Deteccion repetida omitida: " + claveDeteccion);
                }

                responderCliente(clasificacion);
            } catch (Exception e) {
                System.err.println("[" + threadName + "] Error en WorkerTask: " + e.getMessage());
                responderCliente("ERROR");
            }
        }

        private void responderCliente(String clasificacion) {
            if (clientChannel == null || !clientChannel.isOpen()) {
                return;
            }
            try {
                String respuesta = "CLI_RES|" + nodeId + "|0|" + clasificacion + "\n";
                clientChannel.write(ByteBuffer.wrap(respuesta.getBytes(StandardCharsets.UTF_8)));
            } catch (IOException ignored) {
                // La camara puede haber cerrado el socket; no afecta al consenso/log.
            }
        }

        private double[] parseVector(String csv) {
            String[] parts = csv.split(",");
            double[] v = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                v[i] = Double.parseDouble(parts[i].trim());
            }
            return v;
        }

        private String extraerVectorPayload(String payloadCompleto) {
            int pos = payloadCompleto.indexOf(IMG_MARKER);
            if (pos < 0) {
                return payloadCompleto;
            }
            return payloadCompleto.substring(0, pos).trim();
        }

        private String extraerImagenBase64(String payloadCompleto) {
            int pos = payloadCompleto.indexOf(IMG_MARKER);
            if (pos < 0) {
                return "";
            }
            return payloadCompleto.substring(pos + IMG_MARKER.length()).trim();
        }

        private Map<String, double[]> cargarPesos() throws IOException {
            String[] rutas = {PESOS_PATH, "pesos_ia.json", "1-Servidor-Entrenamiento/pesos_ia.json"};
            for (String ruta : rutas) {
                Path path = Paths.get(ruta);
                if (Files.exists(path)) {
                    String contenido = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                    Map<String, double[]> pesos = parsePesosFromJsonContent(contenido);
                    if (!pesos.isEmpty()) {
                        return pesos;
                    }
                }
            }

            Map<String, double[]> fallback = new HashMap<>();
            fallback.put("PERRO", new double[]{121.67, 1.2, 45.0});
            fallback.put("GATO", new double[]{90.0, 0.9, 70.0});
            fallback.put("CARRO", new double[]{200.0, 2.5, 15.0});
            System.err.println("[WorkerTask] pesos_ia.json no encontrado. Usando centroides fallback.");
            return fallback;
        }

        private double distanciaEuclidiana(double[] a, double[] b) {
            double suma = 0;
            int len = Math.min(a.length, b.length);
            for (int i = 0; i < len; i++) {
                suma += Math.pow(a[i] - b[i], 2);
            }
            return Math.sqrt(suma);
        }

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

        private String construirRutaImagen(String camara, String clase, String fecha, int idx) throws IOException {
            Path capturasDir = Paths.get("capturas").toAbsolutePath().normalize();
            Files.createDirectories(capturasDir);

            String fechaArchivo = fecha.replace(":", "-").replace(" ", "_");
            String indice = idx >= 0 ? String.valueOf(idx) : String.valueOf(System.currentTimeMillis());
            String nombre = limpiarNombre(camara) + "_" + fechaArchivo + "_" + indice + "_" + limpiarNombre(clase) + ".png";
            return capturasDir.resolve(nombre).toString();
        }

        private String limpiarNombre(String valor) {
            return valor == null ? "NA" : valor.replaceAll("[^A-Za-z0-9_-]", "_");
        }

        private void guardarCapturaPng(
                Path ruta,
                String camara,
                String clase,
                double[] vector,
                String fecha
        ) throws IOException {
            if (Files.exists(ruta)) {
                return;
            }

            BufferedImage img = new BufferedImage(360, 220, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = img.createGraphics();
            try {
                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                g.setColor(colorBase(clase));
                g.fillRect(0, 0, img.getWidth(), img.getHeight());

                g.setColor(new Color(255, 255, 255, 230));
                g.fillRoundRect(18, 18, 324, 184, 22, 22);

                g.setColor(colorBase(clase).darker());
                g.setFont(new Font("SansSerif", Font.BOLD, 28));
                g.drawString(clase, 36, 62);

                g.setColor(Color.DARK_GRAY);
                g.setFont(new Font("SansSerif", Font.PLAIN, 16));
                g.drawString("Camara: " + camara, 36, 96);
                g.drawString("Fecha: " + fecha, 36, 124);
                g.drawString(String.format("Vector: %.2f, %.3f, %.2f", vector[0], vector[1], vector[2]), 36, 152);
                g.drawString("Formato: PNG", 36, 180);
            } finally {
                g.dispose();
            }

            Files.createDirectories(ruta.getParent());
            ImageIO.write(img, "png", ruta.toFile());
            System.out.println("[WorkerTask] Captura PNG guardada: " + ruta);
        }

        private void guardarCapturaRealPng(Path ruta, String imagenBase64, String clase) throws IOException {
            if (Files.exists(ruta)) {
                return;
            }

            byte[] bytes = Base64.getDecoder().decode(imagenBase64);
            BufferedImage img = ImageIO.read(new ByteArrayInputStream(bytes));
            if (img == null) {
                throw new IOException("La imagen Base64 recibida no pudo decodificarse como PNG");
            }

            Graphics2D g = img.createGraphics();
            try {
                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                
                int w = img.getWidth();
                int h = img.getHeight();
                int boxW = (int) (w * 0.6);
                int boxH = (int) (h * 0.7);
                int x = (w - boxW) / 2;
                int y = (h - boxH) / 2;
                
                g.setStroke(new BasicStroke(4));
                g.setColor(new Color(255, 204, 0));
                g.drawRect(x, y, boxW, boxH);
                
                g.setColor(new Color(255, 204, 0, 220));
                g.fillRect(x, y - 30, 230, 30);
                
                g.setColor(Color.BLACK);
                g.setFont(new Font("SansSerif", Font.BOLD, 22));
                g.drawString("Detecto un: " + clase, x + 10, y - 6);
            } finally {
                g.dispose();
            }

            Files.createDirectories(ruta.getParent());
            ImageIO.write(img, "png", ruta.toFile());
            System.out.println("[WorkerTask] Frame real PNG guardado con BoundingBox: " + ruta);
        }

        private void guardarCapturaTolerante(
                Path ruta,
                String imagenBase64,
                String camara,
                String clase,
                double[] vector,
                String fecha
        ) throws IOException {
            if (imagenBase64 != null && !imagenBase64.isEmpty()) {
                try {
                    guardarCapturaRealPng(ruta, imagenBase64, clase);
                    return;
                } catch (Exception e) {
                    // La imagen no debe tumbar la deteccion. Si el frame Base64
                    // llega cortado/corrupto por red, guardamos respaldo sintetico.
                    System.err.println("[WorkerTask] No se pudo guardar frame real ("
                            + e.getMessage() + "). Usando captura sintetica.");
                }
            }
            guardarCapturaPng(ruta, camara, clase, vector, fecha);
        }

        private Color colorBase(String clase) {
            if ("PERRO".equalsIgnoreCase(clase)) return new Color(46, 204, 113);
            if ("GATO".equalsIgnoreCase(clase)) return new Color(52, 152, 219);
            if ("CARRO".equalsIgnoreCase(clase)) return new Color(230, 126, 34);
            return new Color(149, 165, 166);
        }
    }
}