package uTP.raft;

import uTP.common.Message;
import uTP.network.PeerClient;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class RaftController {
    private final String nodeId;
    private NodeState state = NodeState.FOLLOWER;

    private int currentTerm = 0;
    private String votedFor = null;
    private int votesReceived = 0;
    private String currentLeader = null;

    private Timer timer;
    private final Random random = new Random();

    public RaftController(String nodeId) {
        this.nodeId = nodeId;
        this.timer = new Timer(true); // Hilo daemon
        resetElectionTimer(); // Al iniciar, todos son seguidores esperando al líder
    }

    // ---------------------------------------------------------
    // 1. MANEJO DE TEMPORIZADORES
    // ---------------------------------------------------------

    private void resetElectionTimer() {
        if (timer != null) timer.cancel();
        timer = new Timer(true);

        // Raft usa tiempos aleatorios para evitar empates (ej. 1500ms a 3000ms)
        // Lo ponemos en segundos para que puedas verlo fácilmente en la consola
        int timeout = 1500 + random.nextInt(1500);

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                startElection();
            }
        }, timeout);
    }

    private void startHeartbeatTimer() {
        if (timer != null) timer.cancel();
        timer = new Timer(true);

        // El líder manda un latido cada 800ms
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendHeartbeats();
            }
        }, 0, 800);
    }

    // ---------------------------------------------------------
    // 2. CAMBIOS DE ESTADO (MÁQUINA DE ESTADOS)
    // ---------------------------------------------------------

    private synchronized void startElection() {
        state = NodeState.CANDIDATE;
        currentTerm++;
        votedFor = nodeId; // Voto por mí mismo
        votesReceived = 1;
        currentLeader = null;

        System.out.println("⚠️ [" + nodeId + "] Timeout! Iniciando elección para el término: " + currentTerm);
        resetElectionTimer();

        // Pedir votos a los demás
        Message voteReq = new Message("VOTE_REQ", nodeId, currentTerm, "QUIERO_SER_LIDER");
        broadcast(voteReq);
    }

    private synchronized void becomeLeader() {
        if (state == NodeState.LEADER) return;

        state = NodeState.LEADER;
        currentLeader = nodeId;
        System.out.println("👑 [" + nodeId + "] ¡ME CONVIERTO EN EL LÍDER DEL TÉRMINO " + currentTerm + "! 👑");

        startHeartbeatTimer(); // Empiezo a mandar latidos para mantener el poder
    }

    private synchronized void stepDown(int newTerm) {
        // SOLO si avanzamos al futuro (nuevo mandato), limpiamos nuestro registro de voto
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
        }

        state = NodeState.FOLLOWER;
        resetElectionTimer();
    }

    // ---------------------------------------------------------
    // 3. PROCESAMIENTO DE MENSAJES (Lo que llega de la red)
    // ---------------------------------------------------------

    // Cambiamos el retorno a 'void' ya que las respuestas viajan por canales nuevos
    public synchronized void handleMessage(Message msg) {
        // Si alguien tiene un término mayor, actualizamos nuestro estado
        if (msg.term > currentTerm) {
            stepDown(msg.term);
        }

        switch (msg.type) {
            case "HEARTBEAT":
                if (msg.term >= currentTerm) {
                    currentLeader = msg.senderId;
                    stepDown(msg.term);
                    // El seguidor solo resetea su temporizador. No requiere responder nada.
                }
                break;

            case "VOTE_REQ":
                // Buscamos el puerto del candidato que nos está pidiendo el voto
                Integer puertoCandidato = RaftConfig.PEERS.get(msg.senderId);
                if (puertoCandidato == null) return;

                if ((votedFor == null || votedFor.equals(msg.senderId)) && msg.term >= currentTerm) {
                    votedFor = msg.senderId;
                    resetElectionTimer();
                    System.out.println("🗳️ [" + nodeId + "] Voto CONCEDIDO a " + msg.senderId + " para el término " + msg.term);

                    // ➔ CORRECCIÓN: Devolvemos el voto abriendo un socket nuevo hacia el candidato
                    Message votoSi = new Message("VOTE_RES", nodeId, currentTerm, "TRUE");
                    PeerClient.sendAsync("127.0.0.1", puertoCandidato, votoSi);
                } else {
                    System.out.println("❌ [" + nodeId + "] Voto DENEGADO a " + msg.senderId + " para el término " + msg.term);

                    // También le avisamos que no para que el candidato sepa la realidad asíncronamente
                    Message votoNo = new Message("VOTE_RES", nodeId, currentTerm, "FALSE");
                    PeerClient.sendAsync("127.0.0.1", puertoCandidato, votoNo);
                }
                break;

            case "VOTE_RES":
                // Como las respuestas ahora llegan como mensajes entrantes independientes a nuestro NioServer:
                if (state == NodeState.CANDIDATE && msg.payload.equals("TRUE") && msg.term == currentTerm) {
                    votesReceived++;
                    System.out.println("📈 [" + nodeId + "] Voto recibido de " + msg.senderId + ". Total votos: " + votesReceived + "/3");

                    // Mayoría simple (2 de 3)
                    if (votesReceived > RaftConfig.PEERS.size() / 2) {
                        becomeLeader();
                    }
                }
                break;
        }
    }

    // ---------------------------------------------------------
    // 4. UTILIDAD DE RED
    // ---------------------------------------------------------

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

    // ---------------------------------------------------------
    // 5. APAGADO DE EMERGENCIA (Para simulaciones de caída)
    // ---------------------------------------------------------
    public void stopRaft() {
        if (timer != null) {
            timer.cancel(); // Matamos el hilo de los latidos/elecciones
            timer.purge();
        }
        System.out.println("💀 [" + nodeId + "] Controlador Raft apagado (Muerte simulada).");
    }
}