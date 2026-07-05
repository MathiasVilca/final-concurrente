package uTP.common;

public class Message {
    public String type;      // VOTE_REQ, HEARTBEAT, CLI_REQ, REDIRECT, etc.
    public String senderId;  // Identificador del nodo o cliente.
    public int term;         // Termino actual de Raft.
    public String payload;   // Datos enviados por camaras, nodos o cliente vigilante.

    public Message(String type, String senderId, int term, String payload) {
        this.type = type;
        this.senderId = senderId;
        this.term = term;
        this.payload = payload;
    }

    public String serialize() {
        return type + "|" + senderId + "|" + term + "|" + payload + "\n";
    }

    public static Message deserialize(String rawData) {
        try {
            String[] parts = rawData.split("\\|", 4);

            if (parts.length < 4) {
                System.err.println("[Error] Faltan delimitadores '|' en la trama: " + rawData);
                return null;
            }

            String type = parts[0].trim();
            String senderId = parts[1].trim();
            int term = Integer.parseInt(parts[2].trim());
            String payload = parts[3].trim();

            return new Message(type, senderId, term, payload);

        } catch (NumberFormatException e) {
            System.err.println("[Error] El campo TERM no es un numero entero valido en: " + rawData);
            return null;
        } catch (Exception e) {
            System.err.println("[Error Deserializacion general]: " + e.getMessage());
            return null;
        }
    }
}