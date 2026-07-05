import json
import math

def entrenar_modelo():
    print("=================================================")
    #
    print("   SERVIDOR DE ENTRENAMIENTO DE IA (Puro Python)  ")
    print("=================================================")

    # Dataset de juguete integrado (Características: [Color_Promedio, Aspecto, Textura])
    # Cumple con el requisito de entrenar con un grupo "n" de objetos
    dataset = {
        "PERRO": [
            [120, 1.2, 45], [115, 1.3, 40], [130, 1.1, 50]
        ],
        "GATO": [
            [90, 0.9, 70], [85, 1.0, 75], [95, 0.8, 65]
        ],
        "CARRO": [
            [200, 2.5, 15], [210, 2.4, 20], [190, 2.6, 10]
        ]
    }

    print("[Trainer] Procesando dataset y extrayendo pesos...")
    pesos_modelo = {}

    # Entrenamiento: Calcular el vector promedio (centroide) para cada clase
    for clase, muestras in dataset.items():
        num_muestras = len(muestras)
        suma_caracteristicas = [0.0] * len(muestras[0])

        for muestra in muestras:
            for i in range(len(muestra)):
                suma_caracteristicas[i] += muestra[i]

        # Promediar para obtener el centroide de la clase (estos son tus "pesos")
        centroide = [suma / num_muestras for suma in suma_caracteristicas]
        pesos_modelo[clase] = centroide
        print(f"  ➔ Pesos entrenados para {clase}: {centroide}")

    # Garantizar la persistencia exportando a un archivo JSON físico
    archivo_salida = "pesos_ia.json"
    with open(archivo_salida, "w") as f:
        json.dump(pesos_modelo, f, indent=4)

    print("=================================================")
    print(f"🎉 ¡ENTRENAMIENTO COMPLETADO! Pesos guardados en: {archivo_salida}")
    print("=================================================")

if __name__ == "__main__":
    entrenar_modelo()