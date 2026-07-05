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
        System.out.println("[Launcher]   4. Para tolerancia a fallos, descomenta el bloque al final.\n");

        // -- DEMO DE TOLERANCIA A FALLOS ------------------------------------
        // Para demostrar que Raft elige un nuevo lider cuando cae el actual,
        // descomenta el bloque siguiente JUSTO ANTES de presentar al profesor.
        // Esto mata NODE_1 a los 15s para que los otros dos elijan uno nuevo.
        /*
        try {
            Thread.sleep(15000);
            System.out.println("\n\n[DEMO] !!! MATANDO NODO 1 PARA PROBAR RAFT !!!\n\n");
            nodo1.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
    }
}
