package uTP.raft;

import java.util.HashMap;
import java.util.Map;

public class RaftConfig {
    // Mapa con los IDs de los nodos y sus puertos locales
    public static final Map<String, Integer> PEERS = new HashMap<>();

    static {
        PEERS.put("NODE_1", 8001);
        PEERS.put("NODE_2", 8002);
        PEERS.put("NODE_3", 8003);
    }
}