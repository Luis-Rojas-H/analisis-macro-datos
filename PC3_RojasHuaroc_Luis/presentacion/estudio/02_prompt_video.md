# Prompt 2 — Video (ensayo con diapositivas / Multimodal)

Úsalo en una herramienta que acepte PDF + imagen/video (NotebookLM Video Overview, Gemini, ChatGPT con archivos, etc.). Adjunta:
1. `Guion_Exposicion_PC3.pdf` (esta carpeta)
2. Si puedes: `../Presentacion_PC3.pdf` (las 19 diapositivas)

---

```text
Vas a armar un plan de VIDEO / ensayo visual para mi exposición del Laboratorio 03 (UNI, CC531 A — Análisis en Macrodatos). Te adjunto el PDF del guion oral y, si está disponible, el PDF de la presentación (19 diapositivas).

Objetivo:
Ensayar como si estuviera frente al aula: mirar la diapositiva, señalar, hablar ~10 minutos, sin leer.

Qué debes producir:

1) Storyboard slide a slide (1–19):
   - Qué se VE en la lámina (1 frase).
   - Qué SEÑALO con la mano o el cursor (número, caja Job, tabla, captura).
   - Qué DIGO (máximo 3–5 oraciones, tomadas del guion; no inventes).
   - Quién habla (Luis 1–10 / Breiner 11–19) y duración objetivo.

2) Guion de cámara / postura (texto corto):
   - Dónde mirar (público vs pantalla).
   - Cuándo cambiar de slide.
   - Cómo hacer el pase de voz en la slide 10.

3) Si la herramienta puede generar video o “overview” narrado:
   - Narración alineada al guion, en español, ritmo de exposición universitaria.
   - NO leas tablas enteras ni código línea por línea: solo el fragmento clave (p. ej. reduceByKey, aggregate, join).
   - Destaca visualmente (en la narración) estos anclajes:
     • 30 456 registros
     • promedio vs mediana en 3.b
     • tres jobs de 3.c.1 / 3.c.2 / 3.c.3
     • esquema estrella 53 + 18 + 30 456
     • +121 % y −37 %

4) Errores que el video NO debe cometer:
   - Dec “18 consultas Spark SQL” (son 20, o di “consultas del punto 5”).
   - Presentar DIRPOFIS como “la dependencia con más operativos”.
   - Dec que promedio y media son lo mismo.
   - Dec que Job 2 y Job 3 de 3.c.3 se hacen en el driver con collect/sliding (ahora son sortByKey + join).

5) Cierra con una “lista de corte” de 8 momentos del video donde debo pausar y practicar solo esa frase en voz alta.

No inventes hallazgos fuera del guion. Mantén el límite de 10 minutos.
```
