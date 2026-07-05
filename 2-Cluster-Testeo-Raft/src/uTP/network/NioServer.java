package uTP.network;

import uTP.common.Message;
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

    public NioServer(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            // 1. Abrir Selector y ServerSocketChannel
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false); // Modo NO BLOQUEANTE obligado
            serverChannel.bind(new InetSocketAddress(port));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("[" + nodeId + "] Servidor NIO iniciado en el puerto: " + port);

            // 2. Bucle principal de selección
            while (running) {
                selector.select(); // Espera eventos de red
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
        System.out.println("[" + nodeId + "] Nueva conexión aceptada desde: " + client.getRemoteAddress());
    }

    private void handleRead(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        StringBuilder bufferAcumulador = (StringBuilder) key.attachment();
        ByteBuffer byteBuffer = ByteBuffer.allocate(2048);

        try {
            int bytesRead = client.read(byteBuffer);
            if (bytesRead == -1) { // El cliente cerró la conexión
                System.out.println("[" + nodeId + "] Conexión cerrada por el cliente.");
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

    // Punto crítico donde el Día 2 y 3 inyectarás la lógica de RAFT y HILOS WORKERS
    private void processIncomingMessage(Message msg, SocketChannel client) throws IOException {
        System.out.println("[" + nodeId + "] Mensaje Recibido -> Tipo: " + msg.type + " | De: " + msg.senderId + " | Payload: " + msg.payload);

        // Simulación de respuesta inmediata de Eco por Socket
        if (msg.type.equals("CLI_REQ")) {
            Message response = new Message("CLI_RES", nodeId, 0, "ACK_RECIBIDO_OK");
            client.write(ByteBuffer.wrap(response.serialize().getBytes(StandardCharsets.UTF_8)));
        } else {
            // ➔ ¡ESTO ES LO QUE FALTABA!
            // Si es VOTE_REQ o cualquier otro tipo de control de Raft, le respondemos un ACK genérico
            // para que la consola del cliente reciba datos inmediatamente y no de timeout.
            Message genericAck = new Message("ACK", nodeId, msg.term, "MENSAJE_" + msg.type + "_PROCESADO_OK");
            client.write(ByteBuffer.wrap(genericAck.serialize().getBytes(StandardCharsets.UTF_8)));
        }
    }

    public void shutdown() {
        this.running = false;
        if(selector != null) selector.wakeup();
    }
}

