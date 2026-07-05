package uTP.common;

public class Message {
    public String type;      // VOTE_REQ, HEARTBEAT, CLI_REQ, REDIRECT, etc.
    public String senderId;  // Identificador del nodo (ej: NODE_8001)
    public int term;         // Término actual de Raft
    public String payload;   // Datos, imágenes codificadas en Base64, etc.

    public Message(String type, String senderId, int term, String payload) {
        this.type = type;
        this.senderId = senderId;
        this.term = term;
        this.payload = payload;
    }

    // Serializa el objeto a una trama de texto pura terminada en salto de línea
    public String serialize() {
        return type + "|" + senderId + "|" + term + "|" + payload + "\n";
    }

    // Deserializa una trama recibida por el socket
    public static Message deserialize(String rawData) {
        try {
            // Dividimos por el delimitador |
            String[] parts = rawData.split("\\|");

            if (parts.length < 4) {
                System.err.println("[Error] Faltan delimitadores '|' en la trama: " + rawData);
                return null;
            }

            // Aplicamos trim() a CADA pieza para destruir espacios fantasma o \r de Windows
            String type = parts[0].trim();
            String senderId = parts[1].trim();
            int term = Integer.parseInt(parts[2].trim()); // Blindado contra espacios
            String payload = parts[3].trim();

            return new Message(type, senderId, term, payload);

        } catch (NumberFormatException e) {
            System.err.println("❌ [Error] El campo TERM no es un número entero válido en: " + rawData);
            return null;
        } catch (Exception e) {
            System.err.println("❌ [Error Deserialización general]: " + e.getMessage());
            return null;
        }
    }
}
