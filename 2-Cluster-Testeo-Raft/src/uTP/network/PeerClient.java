package uTP.network;

import uTP.common.Message;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class PeerClient {

    /**
     * Envía un mensaje a otro nodo de forma asíncrona (en un hilo separado)
     * para que nunca bloquee el bucle principal (Selector) de tu servidor.
     */
    public static void sendAsync(String ip, int port, Message msg) {
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                socket.setSoTimeout(1000); // Si el otro nodo está apagado, no se queda colgado

                OutputStream output = socket.getOutputStream();
                String serializedData = msg.serialize();

                output.write(serializedData.getBytes(StandardCharsets.UTF_8));
                output.flush();

                // Nota: No nos quedamos esperando respuesta aquí,
                // ya que Raft maneja las respuestas de forma asíncrona
                // recibiéndolas en el NioServer del nodo remitente.

            } catch (Exception e) {
                // Es normal ver esto si intentamos hablarle a un nodo que está apagado
                System.err.println("[PeerClient] No se pudo enviar mensaje a " + ip + ":" + port + " -> " + e.getMessage());
            }
        }).start();
    }
}
