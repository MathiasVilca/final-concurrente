package uTP;

import uTP.network.NioServer;

public class ClusterLauncher {
    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("   LANZADOR AUTOMÁTICO DEL CLÚSTER RAFT (uTP)    ");
        System.out.println("=================================================");

        // Definimos los 3 nodos que exige la topología mínima de consenso
        NioServer nodo1 = new NioServer("NODE_1", 8001);
        NioServer nodo2 = new NioServer("NODE_2", 8002);
        NioServer nodo3 = new NioServer("NODE_3", 8003);

        // Levantamos cada nodo en su propio hilo (Thread) concurrente
        Thread thread1 = new Thread(nodo1, "Thread-Node-1");
        Thread thread2 = new Thread(nodo2, "Thread-Node-2");
        Thread thread3 = new Thread(nodo3, "Thread-Node-3");

        thread1.start();
        thread2.start();
        thread3.start();

        System.out.println("[Launcher] 3 Nodos levantados concurrentemente.");
        System.out.println("[Launcher] Listo para recibir conexiones de las cámaras Python...\n");

        // Truco para la demostración de tolerancia a fallos ante el profesor:
        // Añadimos un hook para apagar el Nodo 1 simulando una caída tras 15 segundos
        ///*
        try {
            Thread.sleep(5000);
            System.out.println("\n\n⚠️ [SIMULACIÓN] !!! MATANDO NODO 1 (LÍDER) PARA PROBAR RAFT !!! ⚠️\n\n");
            nodo1.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //*/
    }
}
