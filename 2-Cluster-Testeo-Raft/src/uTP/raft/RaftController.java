package uTP.raft;

import uTP.common.Message;
import uTP.network.PeerClient;
import uTP.workers.WorkerPool;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RaftController — versión completa con replicación de log (Día 3)
 *
 * Cambios respecto al Día 2:
 *  - Nuevo estado: commitLog (lista de entradas ya comprometidas)
 *  - Manejo de APPEND_ENTRIES / APPEND_ACK para consenso de datos
 *  - Integración con WorkerPool: al comprometer un dato, lo procesa un Worker
 *  - handleExternalRequest(): punto de entrada para peticiones CLI_REQ externas
 */
public class RaftController {

    // ──────────────────────────────────────────────
    //  Estado RAFT
    // ──────────────────────────────────────────────
    private final String nodeId;
    private NodeState state = NodeState.FOLLOWER;

    private int currentTerm = 0;
    private String votedFor  = null;
    private int votesReceived = 0;
    private String currentLeader = null;

    private Timer timer;
    private final Random random = new Random();

    // ──────────────────────────────────────────────
    //  LOG REPLICADO (Día 3 – Tarea 3.1)
    //  Cada entrada: "CAMARA_1|125,1.25,48"
    // ──────────────────────────────────────────────
    private final List<String> commitLog = new ArrayList<>();

    /**
     * Para el líder: trackea cuántos APPEND_ACK recibimos por cada
     * entrada pendiente antes de comprometer.
     * Clave: índice de la entrada (como String), Valor: ACKs recibidos.
     */
    private final ConcurrentHashMap<Integer, Integer> appendAcks = new ConcurrentHashMap<>();

    /**
     * Guarda el canal del cliente (cámara) para responderle cuando
     * el dato esté comprometido. Clave: índice de la entrada.
     */
    private final ConcurrentHashMap<Integer, SocketChannel> pendingClients = new ConcurrentHashMap<>();

    /**
     * Guarda el payload pendiente de commit. Clave: índice de la entrada.
     */
    private final ConcurrentHashMap<Integer, String[]> pendingEntries = new ConcurrentHashMap<>();

    /**
     * Set de índices ya comprometidos. Previene doble-commit si llegan
     * dos APPEND_ACK para el mismo índice (race condition).
     */
    private final java.util.Set<Integer> committedIndices =
            java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());

    // ──────────────────────────────────────────────
    //  WORKER POOL (Día 3 – Tarea 3.2)
    // ──────────────────────────────────────────────
    private final WorkerPool workerPool;

    public RaftController(String nodeId) {
        this.nodeId   = nodeId;
        this.timer    = new Timer(true);
        this.workerPool = new WorkerPool();
        resetElectionTimer();
    }

    // ─────────────────────────────────────────────────────
    //  1. TEMPORIZADORES
    // ─────────────────────────────────────────────────────

    private void resetElectionTimer() {
        if (timer != null) timer.cancel();
        timer = new Timer(true);
        int timeout = 1500 + random.nextInt(1500);
        timer.schedule(new TimerTask() {
            @Override public void run() { startElection(); }
        }, timeout);
    }

    private void startHeartbeatTimer() {
        if (timer != null) timer.cancel();
        timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override public void run() { sendHeartbeats(); }
        }, 0, 800);
    }

    // ─────────────────────────────────────────────────────
    //  2. MÁQUINA DE ESTADOS
    // ─────────────────────────────────────────────────────

    private synchronized void startElection() {
        state = NodeState.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        votesReceived = 1;
        currentLeader = null;
        System.out.println("⚠️ [" + nodeId + "] Timeout! Iniciando elección para el término: " + currentTerm);
        resetElectionTimer();
        Message voteReq = new Message("VOTE_REQ", nodeId, currentTerm, "QUIERO_SER_LIDER");
        broadcast(voteReq);
    }

    private synchronized void becomeLeader() {
        if (state == NodeState.LEADER) return;
        state = NodeState.LEADER;
        currentLeader = nodeId;
        System.out.println("👑 [" + nodeId + "] ¡ME CONVIERTO EN EL LÍDER DEL TÉRMINO " + currentTerm + "! 👑");
        startHeartbeatTimer();
    }

    private synchronized void stepDown(int newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
        }
        state = NodeState.FOLLOWER;
        resetElectionTimer();
    }

    // ─────────────────────────────────────────────────────
    //  3. PROCESAMIENTO DE MENSAJES
    // ─────────────────────────────────────────────────────

    public synchronized void handleMessage(Message msg) {
        if (msg.term > currentTerm) {
            stepDown(msg.term);
        }

        switch (msg.type) {

            // ── RAFT ORIGINAL ────────────────────────────────
            case "HEARTBEAT":
                if (msg.term >= currentTerm) {
                    currentLeader = msg.senderId;
                    stepDown(msg.term);
                }
                break;

            case "VOTE_REQ":
                Integer puertoCandidato = RaftConfig.PEERS.get(msg.senderId);
                if (puertoCandidato == null) return;
                if ((votedFor == null || votedFor.equals(msg.senderId)) && msg.term >= currentTerm) {
                    votedFor = msg.senderId;
                    resetElectionTimer();
                    System.out.println("🗳️ [" + nodeId + "] Voto CONCEDIDO a " + msg.senderId + " para el término " + msg.term);
                    PeerClient.sendAsync("127.0.0.1", puertoCandidato, new Message("VOTE_RES", nodeId, currentTerm, "TRUE"));
                } else {
                    System.out.println("❌ [" + nodeId + "] Voto DENEGADO a " + msg.senderId);
                    PeerClient.sendAsync("127.0.0.1", puertoCandidato, new Message("VOTE_RES", nodeId, currentTerm, "FALSE"));
                }
                break;

            case "VOTE_RES":
                if (state == NodeState.CANDIDATE && msg.payload.equals("TRUE") && msg.term == currentTerm) {
                    votesReceived++;
                    System.out.println("📈 [" + nodeId + "] Voto recibido de " + msg.senderId + ". Total: " + votesReceived + "/3");
                    if (votesReceived > RaftConfig.PEERS.size() / 2) {
                        becomeLeader();
                    }
                }
                break;

            // ── DÍA 3: REPLICACIÓN DEL LOG ───────────────────
            case "APPEND_ENTRIES":
                /*
                 * El FOLLOWER recibe una entrada del LÍDER.
                 * Payload: "indice:senderId_original:vector"
                 * Ejemplo: "0:CAMARA_1:125,1.25,48"
                 */
                if (msg.term >= currentTerm) {
                    currentLeader = msg.senderId;
                    stepDown(msg.term);

                    // Parsear payload
                    String[] partes = msg.payload.split(":", 3);
                    if (partes.length == 3) {
                        int idx        = Integer.parseInt(partes[0].trim());
                        String entrada = partes[1].trim() + "|" + partes[2].trim();

                        // Guardar en el log local (si no está ya)
                        while (commitLog.size() <= idx) commitLog.add(null);
                        commitLog.set(idx, entrada);

                        System.out.println("[" + nodeId + "] 📋 APPEND_ENTRIES recibido. Log[" + idx + "]=" + entrada);

                        // Responder APPEND_ACK al líder
                        Integer puertoLider = RaftConfig.PEERS.get(msg.senderId);
                        if (puertoLider != null) {
                            Message ack = new Message("APPEND_ACK", nodeId, currentTerm, String.valueOf(idx));
                            PeerClient.sendAsync("127.0.0.1", puertoLider, ack);
                        }
                    }
                }
                break;

            case "APPEND_ACK":
                /*
                 * El LÍDER recibe confirmación de un FOLLOWER.
                 * Cuando llega la mayoría (≥ 2), el dato queda COMMITTED
                 * y se envía al WorkerPool para clasificarlo con IA.
                 */
                if (state == NodeState.LEADER) {
                    int idx = Integer.parseInt(msg.payload.trim());
                    int acksActuales = appendAcks.merge(idx, 1, Integer::sum);
                    System.out.println("[" + nodeId + "] 📨 APPEND_ACK recibido de " + msg.senderId
                            + " para idx=" + idx + " (total ACKs=" + acksActuales + ")");

                    // Mayoría = 2 de 3 (el líder ya contó como 1 implícito)
                    if (acksActuales >= 1) { // 1 follower ACK + líder = mayoría en clúster 3 nodos
                        commitEntry(idx);
                    }
                }
                break;

            case "GET_LOG":
                // Petición del Cliente Vigilante para leer el log
                // La respuesta se maneja en NioServer (ver processIncomingMessage)
                break;
        }
    }

    // ─────────────────────────────────────────────────────
    //  4. PUNTO DE ENTRADA PARA PETICIONES EXTERNAS (CLI_REQ)
    //     Llamado por NioServer cuando llega una cámara
    // ─────────────────────────────────────────────────────

    /**
     * El LÍDER recibe una petición externa de una cámara.
     * Inicia el proceso de replicación vía APPEND_ENTRIES.
     *
     * @param senderId ID de la cámara (ej. "CAMARA_1")
     * @param payload  Vector: "125,1.25,48"
     * @param client   Socket de la cámara para responderle al final
     */
    public synchronized void handleExternalRequest(String senderId, String payload, SocketChannel client) {
        if (state != NodeState.LEADER) {
            // Redirigir al líder (simplificado: avisamos a la cámara)
            System.out.println("[" + nodeId + "] 🔀 No soy el líder. Ignorando petición de " + senderId);
            return;
        }

        // Crear nueva entrada en el log con índice secuencial
        int idx = commitLog.size();
        commitLog.add(null); // placeholder

        // Guardar cliente y payload para responder cuando haya consenso
        pendingClients.put(idx, client);
        pendingEntries.put(idx, new String[]{senderId, payload});
        appendAcks.put(idx, 0);

        System.out.println("[" + nodeId + "] 📤 Replicando entrada[" + idx + "] de " + senderId + ": " + payload);

        // Enviar APPEND_ENTRIES a los followers
        String appendPayload = idx + ":" + senderId + ":" + payload;
        Message append = new Message("APPEND_ENTRIES", nodeId, currentTerm, appendPayload);
        broadcast(append);
    }

    /**
     * Construye el log como String para responder al Cliente Vigilante.
     */
    public synchronized String getLogAsString() {
        if (commitLog.isEmpty() && WorkerPool.resultLog.isEmpty()) {
            return "LOG_VACIO";
        }
        StringBuilder sb = new StringBuilder();
        for (String entry : WorkerPool.resultLog) {
            if (entry != null) sb.append(entry).append(";");
        }
        return sb.length() > 0 ? sb.toString() : "LOG_VACIO";
    }

    // ─────────────────────────────────────────────────────
    //  5. COMMIT: Enviar al WorkerPool cuando hay mayoría
    // ─────────────────────────────────────────────────────

    private void commitEntry(int idx) {
        // Protección contra doble-commit: solo el primer ACK que alcanza mayoría entra
        if (!committedIndices.add(idx)) {
            return; // ya fue comprometido por un ACK anterior
        }
        if (!pendingEntries.containsKey(idx)) return; // entrada ya limpiada

        String[] entry   = pendingEntries.remove(idx);
        SocketChannel ch = pendingClients.remove(idx);
        appendAcks.remove(idx);

        String senderIdCamara = entry[0];
        String vectorPayload  = entry[1];

        // Guardar en log local
        String logEntry = senderIdCamara + "|" + vectorPayload;
        while (commitLog.size() <= idx) commitLog.add(null);
        commitLog.set(idx, logEntry);

        System.out.println("[" + nodeId + "] ✔️ COMMITTED idx=" + idx + ": " + logEntry);

        // Delegar al WorkerPool para clasificar con IA (hilo separado)
        workerPool.submitTask(nodeId, senderIdCamara, vectorPayload, ch);
    }

    // ─────────────────────────────────────────────────────
    //  6. UTILIDAD DE RED
    // ─────────────────────────────────────────────────────

    private void broadcast(Message msg) {
        for (Map.Entry<String, Integer> peer : RaftConfig.PEERS.entrySet()) {
            if (!peer.getKey().equals(nodeId)) {
                PeerClient.sendAsync("127.0.0.1", peer.getValue(), msg);
            }
        }
    }

    private void sendHeartbeats() {
        Message heartbeat = new Message("HEARTBEAT", nodeId, currentTerm, "LATIDO");
        broadcast(heartbeat);
    }

    // ─────────────────────────────────────────────────────
    //  7. ESTADO PÚBLICO (para que NioServer pueda preguntar)
    // ─────────────────────────────────────────────────────

    public boolean isLeader()    { return state == NodeState.LEADER;   }
    public String  getNodeId()   { return nodeId; }
    public String  getLeaderId() { return currentLeader; }
    public NodeState getState()  { return state; }

    // ─────────────────────────────────────────────────────
    //  8. APAGADO
    // ─────────────────────────────────────────────────────

    public void stopRaft() {
        if (timer != null) { timer.cancel(); timer.purge(); }
        workerPool.shutdown();
        System.out.println("💀 [" + nodeId + "] Controlador Raft apagado.");
    }
}