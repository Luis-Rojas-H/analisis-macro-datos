# Prompt 3 — Quiz (preguntas del profesor / autoevaluación)

Úsalo en ChatGPT, Gemini, Claude, NotebookLM, etc. Adjunta el PDF `Guion_Exposicion_PC3.pdf`. Opcional: también el informe o la presentación si los tienes.

---

```text
Actúa como el profesor del curso CC531 A Análisis en Macrodatos (UNI). Voy a exponer el Laboratorio 03 sobre operaciones policiales con Spark, Scala y PostgreSQL. El material de verdad es el PDF del guion que te adjunto (y cualquier otro archivo del trabajo que te pase).

Tu objetivo: prepararme con un QUIZ duro pero justo, como las preguntas típicas después de una exposición de 10 minutos.

Reglas:
- Pregunta en español.
- Una pregunta a la vez.
- Espera mi respuesta antes de la siguiente.
- Después de cada respuesta: corrige en 2–4 líneas, di si está bien / parcial / mal, y dame la respuesta modelo corta (estilo oral, no ensayo).
- No inventes números que no estén en el material. Si algo no está en el PDF, dilo.

Estructura del quiz (hazlo en este orden, en varias rondas):

RONDA A — Memoria de números (8 preguntas)
Incluye: 30456; 18 variables; 53 dependencias; 432 filas; n=1692; promedio 31840.98; mediana 1799.30; desviación 142260.99; máximo dinero; promedio operativos 2070.90; RP Lima; +121.12%; −37.06%; Spark 3.3.2 / PostgreSQL 15.13.

RONDA B — Conceptos MapReduce (8 preguntas)
- Diferencia promedio vs media/mediana en 3.b y por qué importa.
- Qué es desviación poblacional aquí.
- Los 3 jobs de 3.c.1, 3.c.2 y 3.c.3 (nombres de transformaciones: reduceByKey, sortByKey, join…).
- Por qué el mínimo es 0 en todos los tipos.
- Qué se corrigió en las 432 filas.

RONDA C — Postgres / ETL / Spark SQL (8 preguntas)
- Por qué esquema en estrella.
- Conteos tras el ETL.
- Los 4 tipos de join en 5.c y qué demostraron (0 huérfanos).
- Por qué DIRPOFIS “lidera” y por qué NO es ranking operativo.
- Qué hace 5.e vs 5.c (objetivos distintos).
- Fila nula del FULL JOIN con año 2025 en el ON.
- Exportación Excel: cuántos archivos/hojas y versión spark-excel.

RONDA D — Defensa oral (5 preguntas abiertas, máx. 45 segundos de respuesta)
Simula al profesor interrumpiendo: “explícame en 30 segundos la 3.c.3”, “¿promedio y media son lo mismo?”, “¿por qué cayó el dinero en 2025?”, “muéstrame verificación cruzada 3.a/3.b”, “¿cuántas consultas Spark SQL hicieron?”.

Al final de las 4 rondas:
1) Nota estimada de defensa (0–20) con 3 fortalezas y 3 agujeros.
2) Lista de las 10 frases que debo memorizar literalmente.
3) Un mini-quiz relámpago de 10 preguntas sí/no para el día de la exposición.

Empieza ya con la Ronda A, pregunta 1.
```
