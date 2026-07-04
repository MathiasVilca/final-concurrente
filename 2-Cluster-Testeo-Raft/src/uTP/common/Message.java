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
            String[] parts = rawData.trim().split("\\|");
            if (parts.length < 4) return null;
            return new Message(parts[0], parts[1], Integer.parseInt(parts[2]), parts[3]);
        } catch (Exception e) {
            System.err.println("[Error Deserialización]: Mensaje malformado -> " + rawData);
            return null;
        }
    }
}
