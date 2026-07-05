# -*- coding: utf-8 -*-
"""
1-Servidor-Entrenamiento/model_trainer.py
=========================================
Servidor de Entrenamiento de IA.
Calcula el centroide (vector promedio) de cada clase como "pesos" del modelo.
Persiste los pesos en pesos_ia.json para que los Workers del cluster los usen.

Clases entrenadas (n=3): PERRO, GATO, CARRO
Caracteristicas: [Color_Promedio, Aspecto, Textura]
"""

import json
import os
import sys

# Fix de codificacion en Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def entrenar_modelo():
    print("=================================================")
    print("   SERVIDOR DE ENTRENAMIENTO DE IA (Puro Python) ")
    print("=================================================")

    # Dataset de juguete integrado
    # Caracteristicas: [Color_Promedio, Aspecto, Textura]
    # Cumple el requisito de entrenar con grupo n=3 de objetos/animales
    dataset = {
        "PERRO": [
            [120, 1.2, 45],
            [115, 1.3, 40],
            [130, 1.1, 50],
        ],
        "GATO": [
            [90, 0.9, 70],
            [85, 1.0, 75],
            [95, 0.8, 65],
        ],
        "CARRO": [
            [200, 2.5, 15],
            [210, 2.4, 20],
            [190, 2.6, 10],
        ],
    }

    print("[Trainer] Procesando dataset y calculando centroides...")
    pesos_modelo = {}

    # Entrenamiento: calcular el vector promedio (centroide) de cada clase
    for clase, muestras in dataset.items():
        n = len(muestras)
        suma = [0.0] * len(muestras[0])
        for muestra in muestras:
            for i, v in enumerate(muestra):
                suma[i] += v
        centroide = [s / n for s in suma]
        pesos_modelo[clase] = centroide
        print(f"  -> Pesos de {clase}: {[round(c, 4) for c in centroide]}")

    # Persistencia: exportar pesos a JSON para consumo por los Workers del cluster
    # Guardar siempre junto a este script. Asi funciona igual si se ejecuta
    # desde la raiz del proyecto o desde 1-Servidor-Entrenamiento/.
    archivo_salida = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pesos_ia.json")
    with open(archivo_salida, "w", encoding="utf-8") as f:
        json.dump(pesos_modelo, f, indent=4)

    print("=================================================")
    print(f"[OK] ENTRENAMIENTO COMPLETADO. Pesos guardados en: {archivo_salida}")
    print("=================================================")
    return pesos_modelo


if __name__ == "__main__":
    entrenar_modelo()