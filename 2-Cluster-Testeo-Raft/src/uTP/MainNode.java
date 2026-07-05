package uTP;

import uTP.network.NioServer;

public class MainNode {
    public static void main(String[] args) {
        // Valores por defecto si no pasas parámetros por consola de IntelliJ
        String nodeId = "NODE_1";
        int port = 8001;

        if (args.length >= 2) {
            nodeId = args[0];
            port = Integer.parseInt(args[1]);
        }

        System.out.println("--- INICIANDO NODO DEL CLUSTER ---");
        NioServer server = new NioServer(nodeId, port);

        // Ejecutar el servidor NIO en un hilo independiente para que no bloquee la consola
        Thread serverThread = new Thread(server);
        serverThread.start();
    }
}