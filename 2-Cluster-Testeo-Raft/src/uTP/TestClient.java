package uTP;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class TestClient {
    public static void main(String[] args) {
        Scanner consoleScanner = new Scanner(System.in);
        System.out.println("=================================================");
        System.out.println("     CONSOLA INTERACTIVA DE PRUEBA (DEBUG)       ");
        System.out.println("=================================================");

        while (true) {
            System.out.print("Escribe tu trama de mensaje: ");
            String rawInput = consoleScanner.nextLine();

            if (rawInput.equalsIgnoreCase("salir")) break;
            if (rawInput.trim().isEmpty()) continue;

            if (!rawInput.endsWith("\n")) {
                rawInput += "\n";
            }

            System.out.println("[DEBUG 1] Intentando conectar al puerto 8001...");
            try (Socket socket = new Socket("127.0.0.1", 8001)) {
                System.out.println("[DEBUG 2] ¡Conectado con éxito! Configurando timeout de 2s...");
                socket.setSoTimeout(2000);

                System.out.println("[DEBUG 3] Obteniendo OutputStream y enviando bytes...");
                OutputStream output = socket.getOutputStream();
                output.write(rawInput.getBytes(StandardCharsets.UTF_8));
                output.flush();
                System.out.println("[DEBUG 4] Bytes enviados correctamente a la red.");

                System.out.println("[DEBUG 5] Esperando respuesta en el InputStream (Aquí solía congelarse)...");
                InputStream input = socket.getInputStream();
                byte[] responseBuffer = new byte[1024];

                // Línea crítica de bloqueo
                int bytesRead = input.read(responseBuffer);
                System.out.println("[DEBUG 6] ¡Lectura terminada! Bytes leídos: " + bytesRead);

                if (bytesRead != -1) {
                    String response = new String(responseBuffer, 0, bytesRead, StandardCharsets.UTF_8);
                    System.out.println("➔ [RESPUESTA DEL NODO]: " + response.trim());
                } else {
                    System.out.println("➔ [INFO]: El servidor cerró el canal.");
                }

            } catch (java.net.SocketTimeoutException e) {
                System.err.println("⏱️ [DEBUG TIMEOUT]: El servidor no respondió en 2 segundos. Rompiendo bloqueo.");
            } catch (Exception e) {
                System.err.println("❌ [DEBUG ERROR]: Falló la conexión -> " + e.getMessage());
            }
            System.out.println("-------------------------------------------------");
        }
        consoleScanner.close();
    }
}