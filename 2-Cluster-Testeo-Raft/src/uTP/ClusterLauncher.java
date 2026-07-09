package uTP;

import uTP.network.NioServer;

public class ClusterLauncher {
    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("   LANZADOR AUTOMATICO DEL CLUSTER RAFT (uTP)    ");
        System.out.println("   Dias 3+4: Workers IA + Replicacion de Log     ");
        System.out.println("=================================================");

        // -- 3 Nodos del cluster -------------------------------------------
        NioServer nodo1 = new NioServer("NODE_1", 8001);
        NioServer nodo2 = new NioServer("NODE_2", 8002);
        NioServer nodo3 = new NioServer("NODE_3", 8003);

        // Cada nodo corre en su propio hilo (requisito de concurrencia)
        Thread thread1 = new Thread(nodo1, "Thread-Node-1");
        Thread thread2 = new Thread(nodo2, "Thread-Node-2");
        Thread thread3 = new Thread(nodo3, "Thread-Node-3");

        thread1.start();
        thread2.start();
        thread3.start();

        System.out.println("[Launcher] OK 3 Nodos levantados concurrentemente.");
        System.out.println("[Launcher] OK WorkerPool IA iniciado en cada nodo.");
        System.out.println("[Launcher] OK Listo para recibir camaras Python.\n");
        System.out.println("[Launcher] Instrucciones:");
        System.out.println("[Launcher]   1. Espera ~2s a que el cluster elija un LIDER.");
        System.out.println("[Launcher]   2. Ejecuta: python 3-Emulador-Camaras/camera_client.py");
        System.out.println("[Launcher]   3. Ejecuta: python 4-Cliente-Vigilante/vigilante_app.py");
        System.out.println("[Launcher]   4. Para tolerancia a fallos: java -cp out uTP.ClusterLauncher --demo-failover\n");

        if (tieneArg(args, "--demo-failover")) {
            long delayMs = leerDelay(args, 15000);
            Thread demo = new Thread(() -> ejecutarDemoFailover(delayMs, nodo1, nodo2, nodo3), "Demo-Failover");
            demo.setDaemon(true);
            demo.start();
        }
    }

    private static boolean tieneArg(String[] args, String buscado) {
        for (String arg : args) {
            if (buscado.equals(arg)) return true;
        }
        return false;
    }

    private static long leerDelay(String[] args, long defaultMs) {
        for (String arg : args) {
            if (arg.startsWith("--failover-delay-ms=")) {
                try {
                    return Long.parseLong(arg.substring("--failover-delay-ms=".length()));
                } catch (NumberFormatException ignored) {
                    return defaultMs;
                }
            }
        }
        return defaultMs;
    }

    private static void ejecutarDemoFailover(long delayMs, NioServer... nodos) {
        try {
            Thread.sleep(delayMs);
            NioServer lider = null;
            for (NioServer nodo : nodos) {
                if (nodo.isLeader()) {
                    lider = nodo;
                    break;
                }
            }
            if (lider == null) {
                lider = nodos[0];
                System.out.println("[DEMO] No se detecto lider activo; apagando " + lider.getNodeId() + " como fallback.");
            } else {
                System.out.println("[DEMO] Apagando lider activo " + lider.getNodeId() + " para probar Raft.");
            }
            lider.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
