package uTP.network;

import uTP.common.Message;
import uTP.raft.RaftController;
import uTP.raft.RaftConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class NioServer implements Runnable {
    private final int port;
    private final String nodeId;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private boolean running = true;
    private RaftController raftController;

    public NioServer(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
        this.raftController = new RaftController(nodeId);
    }

    @Override
    public void run() {
        try {
            // 1. Abrir Selector y ServerSocketChannel
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false); // Modo NO BLOQUEANTE obligado
            serverChannel.bind(new InetSocketAddress(RaftConfig.LISTEN_HOST, port));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("[" + nodeId + "] Servidor NIO iniciado en " + RaftConfig.LISTEN_HOST + ":" + port);

            // 2. Bucle principal de selección
            while (running) {
                selector.select(300); // timeout 300ms: el selector no bloquea indefinidamente
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) continue;

                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        client.configureBlocking(false);
        // Registramos el cliente para lectura y le adjuntamos un StringBuilder para acumular los datos
        client.register(selector, SelectionKey.OP_READ, new StringBuilder());
        //Comentadooooo
        //System.out.println("[" + nodeId + "] Nueva conexión aceptada desde: " + client.getRemoteAddress());
    }

    private void handleRead(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        StringBuilder bufferAcumulador = (StringBuilder) key.attachment();
        ByteBuffer byteBuffer = ByteBuffer.allocate(65536); // Grande para payloads Base64

        try {
            int bytesRead = client.read(byteBuffer);
            if (bytesRead == -1) { // El cliente cerró la conexión
                //when comentas
                //System.out.println("[" + nodeId + "] Conexión cerrada por el cliente.");
                client.close();
                key.cancel();
                return;
            }

            byteBuffer.flip();
            String fragmento = new String(byteBuffer.array(), 0, bytesRead, StandardCharsets.UTF_8);
            bufferAcumulador.append(fragmento);

            // Procesar si encontramos un fin de línea \n (Raft / Mensajes de texto)
            String datos = bufferAcumulador.toString();
            if (datos.contains("\n")) {
                int indexSalto = datos.indexOf("\n");
                String mensajeCompleto = datos.substring(0, indexSalto);
                // Guardamos el sobrante en el acumulador por si llegaron partes de otro mensaje
                bufferAcumulador.setLength(0);
                bufferAcumulador.append(datos.substring(indexSalto + 1));

                // Deserializar trama
                Message msg = Message.deserialize(mensajeCompleto);
                if (msg != null) {
                    processIncomingMessage(msg, client);
                }
            }

        } catch (IOException e) {
            System.err.println("[" + nodeId + "] Error leyendo del cliente, cerrando socket.");
            try { client.close(); } catch (IOException ignored) {}
            key.cancel();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DÍA 3: Punto de integración entre la red y la lógica Raft + Workers
    // ─────────────────────────────────────────────────────────────────────────
    private void processIncomingMessage(Message msg, SocketChannel client) throws IOException {

        if (!msg.type.equals("HEARTBEAT")) {
            System.out.println("[" + nodeId + "] Mensaje Recibido -> Tipo: " + msg.type
                    + " | De: " + msg.senderId
                    + " | Payload: " + msg.payload.substring(0, Math.min(60, msg.payload.length())));
        }

        // ── 1. CLI_REQ: Petición de cámara → pasar al RaftController para replicar ─────
        if (msg.type.equals("CLI_REQ")) {
            if (msg.payload.equals("GET_LOG")) {
                // El Cliente Vigilante pide el log completo
                String logData = raftController.getLogAsString();
                String respuesta = "CLI_RES|" + nodeId + "|0|" + logData + "\n";
                try {
                    client.write(ByteBuffer.wrap(respuesta.getBytes(StandardCharsets.UTF_8)));
                } catch (IOException ignored) {}
                return;
            }

            // Es una petición de clasificación de cámara
            if (raftController.isLeader()) {
                // Líder: iniciar replicación Raft y luego clasificar con IA
                raftController.handleExternalRequest(msg.senderId, msg.payload, client);
            } else {
                // No soy el líder: avisar a la cámara que se reconecte al líder
                String lider = raftController.getLeaderId();
                String info  = (lider != null) ? "LIDER_ES:" + lider : "SIN_LIDER";
                System.out.println("[" + nodeId + "] [REDIR] No soy lider. Redirigiendo " + msg.senderId + " -> " + info);
                Message redir = new Message("REDIRECT", nodeId, 0, info);
                try {
                    client.write(ByteBuffer.wrap(redir.serialize().getBytes(StandardCharsets.UTF_8)));
                } catch (IOException ignored) {}
            }
            return;
        }

        // ── 2. Mensajes internos Raft (VOTE_REQ, VOTE_RES, HEARTBEAT, APPEND_ENTRIES, APPEND_ACK) ─
        raftController.handleMessage(msg);
    }
    public void shutdown() {
        running = false;
        try {
            if (selector != null) selector.wakeup();
            if (serverChannel != null) serverChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // ➔ NUEVO: Apagamos también el cerebro del nodo para que no queden hilos zombies
        if (raftController != null) {
            raftController.stopRaft();
        }
    }

    public boolean isLeader() {
        return raftController != null && raftController.isLeader();
    }

    public String getNodeId() {
        return nodeId;
    }
}

