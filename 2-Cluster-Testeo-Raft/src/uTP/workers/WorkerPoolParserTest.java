package uTP.workers;

import java.util.Map;

public class WorkerPoolParserTest {
    public static void main(String[] args) {
        String contenido = "{\n" +
                "  \"PERRO\": [121.66666666666667, 1.2, 45.0],\n" +
                "  \"GATO\": [90.0, 0.9, 70.0],\n" +
                "  \"CARRO\": [200.0, 2.5, 15.0]\n" +
                "}";

        Map<String, double[]> pesos = WorkerPool.parsePesosFromJsonContent(contenido);

        if (pesos.size() != 3) {
            throw new AssertionError("Se esperaban 3 clases, se cargaron " + pesos.size());
        }
        if (!pesos.containsKey("PERRO") || !pesos.containsKey("GATO") || !pesos.containsKey("CARRO")) {
            throw new AssertionError("No se cargaron todas las clases esperadas");
        }
        if (pesos.get("PERRO").length != 3 || pesos.get("GATO").length != 3 || pesos.get("CARRO").length != 3) {
            throw new AssertionError("Los centroides deben tener 3 valores por clase");
        }
        if (Math.abs(pesos.get("PERRO")[0] - 121.66666666666667) > 1e-9) {
            throw new AssertionError("El centroide de PERRO no se parseo correctamente");
        }
        if (Math.abs(pesos.get("GATO")[1] - 0.9) > 1e-9) {
            throw new AssertionError("El centroide de GATO no se parseo correctamente");
        }
        if (Math.abs(pesos.get("CARRO")[2] - 15.0) > 1e-9) {
            throw new AssertionError("El centroide de CARRO no se parseo correctamente");
        }

        System.out.println("Parser de pesos OK");
    }
}