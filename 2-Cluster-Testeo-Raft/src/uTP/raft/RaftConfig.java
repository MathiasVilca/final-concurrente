package uTP.raft;

import java.util.HashMap;
import java.util.Map;

public class RaftConfig {
    // Host donde cada nodo abre su socket servidor. "0.0.0.0" permite
    // recibir conexiones desde otras maquinas de la LAN/WIFI.
    public static final String LISTEN_HOST = getConfig("RAFT_LISTEN_HOST", "0.0.0.0");

    // Mapa con los IDs de los nodos y sus puertos.
    public static final Map<String, Integer> PEERS = new HashMap<>();
    public static final Map<String, String> HOSTS = new HashMap<>();

    static {
        PEERS.put("NODE_1", 8001);
        PEERS.put("NODE_2", 8002);
        PEERS.put("NODE_3", 8003);

        String hostDefault = getConfig("RAFT_HOST_ALL", "127.0.0.1");
        HOSTS.put("NODE_1", getConfig("RAFT_HOST_NODE_1", hostDefault));
        HOSTS.put("NODE_2", getConfig("RAFT_HOST_NODE_2", hostDefault));
        HOSTS.put("NODE_3", getConfig("RAFT_HOST_NODE_3", hostDefault));
    }

    public static String hostFor(String nodeId) {
        String host = HOSTS.get(nodeId);
        return host == null || host.trim().isEmpty() ? "127.0.0.1" : host;
    }

    private static String getConfig(String key, String defaultValue) {
        String prop = System.getProperty(key);
        if (prop != null && !prop.trim().isEmpty()) {
            return prop.trim();
        }
        String env = System.getenv(key);
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        return defaultValue;
    }
}
