# Guion de exposición — Laboratorio 03 (PC3)

**Curso:** CC531 A — Análisis en Macrodatos 2026-2  
**Integrantes:** Luis Antonio Rojas Huaroc · Breiner Catalino Morales  
**Duración objetivo:** 9:30–10:00 (el límite es 10 minutos; si se pasan, descuentan)  
**Presentación:** `Presentacion_PC3.pdf` (19 diapositivas)

---

## Cómo usarlo

- No lean la diapositiva. Señalen el número, el diagrama o el resultado y digan el texto.
- Lo que está entre corchetes `[...]` se puede saltar si el reloj va justo.
- Ensayen una vez de pie, con cronómetro. Si a la diapositiva 10 van por los 5 minutos, van bien.

| Diapositiva | Quién | Tiempo |
|---|---|---|
| 1. Portada | Luis | 0:20 |
| 2. Qué exige el laboratorio | Luis | 0:20 |
| 3. El dataset | Luis | 0:40 |
| 4. Entorno | Luis | 0:15 |
| 5. Las cinco consultas RDD | Luis | 0:25 |
| 6. 3.a | Luis | 0:35 |
| 7. 3.b | Luis | 0:50 |
| 8. 3.c.1 | Luis | 0:40 |
| 9. 3.c.2 | Luis | 0:40 |
| 10. 3.c.3 | Luis | 0:40 |
| 11. Esquema en estrella | Breiner | 0:35 |
| 12. ETL | Breiner | 0:30 |
| 13. Evidencia | Breiner | 0:15 |
| 14. 5.a y 5.b | Breiner | 0:40 |
| 15. 5.c y 5.d | Breiner | 0:35 |
| 16. 5.e y 5.f | Breiner | 0:35 |
| 17. Excel | Breiner | 0:15 |
| 18. Conclusiones (1/2) | Breiner | 0:20 |
| 19. Conclusiones (2/2) | Breiner | 0:25 |

**Reparto:** Luis diapositivas 1–10 · Breiner diapositivas 11–19.

---

## Texto por diapositiva

### 1. Portada — Luis

Buenos días. Somos Luis Rojas y Breiner Morales, de Ciencias de la Computación. Exponemos el laboratorio 3 de Análisis en Macrodatos. Trabajamos con Resultados de las operaciones policiales a nivel nacional, del MININTER, usando Spark, Scala y PostgreSQL.

### 2. Qué exige este laboratorio — Luis

El enunciado pide cinco cosas, y las vamos en ese orden: un dataset de Seguridad y Justicia que no haya usado otro grupo, MapReduce en Scala sobre RDD, una base en PostgreSQL con su ETL, consultas en Spark SQL, y exportar a Excel desde Spark.

### 3. El dataset: cómo lo elegimos — Luis

Primero pensamos en Denuncias policiales del MININTER, pero otro grupo ya la tenía. Nos quedamos con los resultados de operaciones policiales de CENOPPOL, en la Plataforma Nacional de Datos Abiertos. Unimos 2023, 2024 y 2025: 30 456 registros. Cada fila es dependencia, mes, tipo de resultado y valor. Hay 18 variables y 53 dependencias. El valor es decimal: kilos, soles, personas o unidades, según el tipo.

### 4. Entorno instalado — Luis

Versiones de clase: Spark 3.3.2, Scala 2.12.15, PostgreSQL 15.13 y el driver JDBC 42.7.6. Todo en local, con binarios portátiles.

### 5. Las cinco consultas RDD / MapReduce — Luis

Sobre RDD hicimos cinco consultas. La 3.a saca el mayor y el menor por tipo. La 3.b saca promedio, mediana, desviación, mayor y menor. Las tres de 3.c encadenan tres MapReduce con shuffle: primero agregan, luego el agregado global, y al final el resultado. Las tres usan decimales.

### 6. 3.a — Mayor/menor por tipo — Luis

¿Cuál es el mayor y el menor valor de cada tipo entre 2023 y 2025? Mapeamos variable–valor y dos `reduceByKey`, con `max` y con `min`. El mínimo es 0 en los 18 tipos: hay meses sin incidencia. Máximos relevantes: dinero, 4 028 271.78 soles; marihuana, 29 924.75 kilos; contrabando, cerca de 514 millones. El máximo de dinero lo volvemos a ver en la 3.b.

### 7. 3.b — Estadísticos del dinero — Luis

El enunciado pide promedio y media por separado. Aquí el promedio es la media aritmética: 31 840.98 soles por registro. La media la tomamos como mediana: 1 799.30 soles. La mediana sale ordenando el RDD y promediando los dos valores centrales, porque n es par, 1 692. El `aggregate` acumula suma, suma de cuadrados, conteo, máximo y mínimo; de ahí salen promedio, desviación, mayor y menor. La desviación es 142 260.99, más de cuatro veces el promedio. La mediana queda muy por debajo del promedio: la distribución está sesgada. Muchos montos son chicos o cero, y unos pocos operativos grandes jalan el promedio. El máximo coincide exacto con la 3.a. La desviación es poblacional, dividida entre n, porque describimos todo el periodo.

### 8. 3.c.1 — Desviación de OPERATIVOS — Luis

¿Qué dependencias se alejan del promedio nacional de operativos? Job 1: `reduceByKey` de suma y conteo por dependencia. Job 2: reduce a un solo promedio nacional. Job 3: resta ese promedio al de cada dependencia y ordena por valor absoluto. Promedio nacional: 2 070.90. RP Lima queda 10 606 por encima. Siguen Cusco y Áncash. DIRFAM queda en cero: no reporta operativos territoriales.

### 9. 3.c.2 — Porcentaje de droga por región — Luis

Solo kilos de PBC, marihuana y clorhidrato. Job 1: suma por región y droga. Job 2: total por región. Job 3: porcentaje. En DIRNIC la marihuana es 36 %, pero las tres están repartidas. En los frentes manda el clorhidrato, 83 %. En las regiones, la marihuana, 83 %. [DIRNIC opera a nivel nacional contra redes más grandes, por eso el decomiso no se concentra en una sola droga.] Antes de agregar corregimos 432 filas de 2025: en `DIR_REG_FRE` venía el nombre de la dependencia en vez de Región.

### 10. 3.c.3 — Variación interanual del dinero — Luis

¿Subió o bajó el promedio mensual de dinero incautado? Job 1: `reduceByKey` suma el dinero por año. Job 2: promedio mensual = suma del año entre 12, y `sortByKey` deja el RDD ordenado en el clúster. Job 3: `join` del promedio de un año contra el del siguiente, con la clave desplazada, y de ahí el porcentaje. De 2023 a 2024 sube 121 %. De 2024 a 2025 baja 37 %. Los tres años tienen los doce meses, por eso dividimos entre 12. [En 2025 reportan 49 dependencias y en los años previos 46, así que la caída no es sobre exactamente el mismo conjunto.] El monto es volátil, y cuadra con la desviación y con la brecha promedio–mediana de la 3.b.

**Paso de voz:** «Breiner, sigue con la base.»

### 11. Arquitectura: esquema en estrella — Breiner

Para guardarlo armamos un esquema en estrella: dos dimensiones y una tabla de hechos. `dim_dependencia` tiene 53 filas, con el nombre y si es región, frente, DIRNIC o DIRNOS. `dim_variable` tiene las 18 variables, más categoría y unidad. El hecho guarda año, mes, las dos llaves y el valor: las mismas 30 456 filas del CSV. Cada dimensión se relaciona uno a muchos con el hecho.

### 12. ETL: Extract – Transform – Load — Breiner

El ETL está en Scala. Extract lee los tres CSV ya combinados. Transform corrige esas 432 filas, deriva categoría y unidad con `when`, y asigna las llaves con `row_number` para que el identificador no cambie de una corrida a otra. Load escribe las tres tablas por JDBC, encima del `CREATE TABLE`. El volcado de PostgreSQL va en la entrega.

### 13. Evidencia: ETL y base de datos — Breiner

Esta captura es después de la carga: 53 dependencias, 18 variables y 30 456 hechos. El conteo de la tabla y el del CSV coinciden.

### 14. Spark SQL — 5.a y 5.b — Breiner

Para Spark SQL leemos las tres tablas por JDBC y armamos un detalle uniendo el hecho con las dos dimensiones. La 5.a son cuatro `groupBy` con `filter`, siempre sobre dos columnas o más: suma por año y categoría, conteo por dependencia y mes, kilos por región, y total por año y unidad. La 5.b tiene el promedio de valor por categoría y, en otra consulta, las dependencias ordenadas de mayor a menor. Arriba sale DIRPOFIS, cerca de 630 millones. Esa suma la domina el contrabando en soles. No es un ranking de operativos, porque está mezclando unidades distintas.

### 15. Spark SQL — 5.c y 5.d — Breiner

La 5.c es un join de cada tipo: inner, left, right y full. Los cuatro devuelven 30 456 filas. Con el left y el full buscamos dependencias o variables sin ningún hecho, y salieron cero. El ETL no dejó llaves huérfanas. La 5.d usa `org.apache.spark.sql.functions`: `round` del promedio, `when` para clasificar el valor en rangos, `concat_ws` para armar una etiqueta, y `stddev` por categoría. En los rangos, 12 137 registros quedan en cero. Es lo esperable cuando se cruza dependencia, mes y tipo: muchos de esos cruces no tienen incidencia ese mes.

### 16. Vistas temporales — 5.e y 5.f — Breiner

Acá el SQL va sobre vistas temporales, con `spark.sql`, y las preguntas son distintas a las de los joins anteriores. En la 5.e: un inner para ver qué dependencia incautó más dinero en cada año, un left para el total de operativos por dependencia, y un full para ver si alguna variable no tuvo registros en 2025. Las 18 tuvieron. En la 5.f hay tres `ORDER BY` con filtro: armas de mayor a menor, registros de región en 2025 de mayor a menor, y dinero incautado mayor que cero ordenado de menor a mayor.

### 17. Exportación a Excel (5.g) — Breiner

Esas seis consultas se escribieron con `spark-excel` 3.3.2. Un archivo para la 5.e y otro para la 5.f, tres hojas cada uno.

### 18. Conclusiones (1/2) — Breiner

Cerramos con esto. Quedaron las cinco consultas MapReduce sobre los 30 456 registros, y las del punto 5 en DataFrame y vistas temporales. *(No digan «18 consultas». Si preguntan el número: 4 + 2 + 4 + 4 + 3 + 3 = 20.)* La base está en estrella, cargada por el ETL, y en el camino corregimos un error real del CSV.

### 19. Conclusiones (2/2) — Breiner

Lo verificamos cruzando consultas: el máximo de dinero de la 3.a y el de la 3.b son el mismo número, y los cuatro joins no encuentran registros huérfanos. El resultado que más se nota es el dinero mensual: en 2024 creció 121 % y en 2025 cayó 37 %, en línea con una desviación varias veces el promedio y con una mediana mucho más baja. Gracias. Quedamos atentos a las preguntas.

---

## Preguntas frecuentes (respuestas cortas)

Si no la saben, digan que lo revisan en el código. No inventen.

| Pregunta | Respuesta |
|---|---|
| ¿Por qué el mínimo es 0 en todos los tipos? | Porque el dataset trae el mes aunque no haya habido incautación. El cero es un registro real, no un nulo. |
| ¿Por qué promedio y media no son lo mismo? | El enunciado los pide por separado. Promedio = media aritmética (S/ 31 840.98). Media = mediana (S/ 1 799.30). La mediana describe mejor el registro típico; el promedio lo jala el máximo. |
| ¿La desviación es muestral o poblacional? | Poblacional: dividimos entre *n*, porque describimos todo el periodo 2023–2025, no una muestra. |
| ¿La 3.c.3 son tres MapReduce reales? | Sí. Job 1: `reduceByKey` por año. Job 2: promedio mensual con `sortByKey` (shuffle). Job 3: `join` del año A contra el A+1 y el porcentaje en el `map`. |
| ¿Por qué DIRPOFIS lidera en 5.b? | Porque la suma junta soles de contrabando con kilos y conteos. El contrabando aplasta al resto. No es un ranking de operativos. |
| ¿Qué pasó con las 432 filas? | En 2025, RP Ica y RP Tumbes tenían su propio nombre en `DIR_REG_FRE`. Las pasamos a Región, en el ETL y en la consulta de drogas. |
| ¿Por qué estrella y no una sola tabla? | Dependencia y variable se repiten cada mes. Dejarlas en dimensión evita repetir el nombre y permite agrupar por categoría o región sin parsear el texto otra vez. |
| ¿Y la fila nula del FULL JOIN en 5.e.3? | El año 2025 va en el `ON`. Las 18 variables sí tienen registros ese año. La fila nula son hechos de 2023 y 2024 que no empatan con esa condición. |
| ¿Por qué cayó el dinero en 2025? | El promedio mensual bajó 37 %. En 2025 hay 49 dependencias y antes 46; sin más contexto (campañas, cambios normativos) no atribuimos la caída a un solo factor. |

---

## Números clave para memorizar

| Dato | Valor |
|---|---|
| Registros | 30 456 |
| Variables / dependencias | 18 / 53 |
| Filas mal copiadas (2025) | 432 |
| Dinero — n | 1 692 |
| Dinero — promedio | S/ 31 840.98 |
| Dinero — mediana | S/ 1 799.30 |
| Dinero — desviación | S/ 142 260.99 |
| Dinero — máximo | S/ 4 028 271.78 |
| Operativos — promedio nacional | 2 070.90 |
| Operativos — RP Lima (desviación) | +10 606.94 |
| Variación dinero 2023→2024 | +121.12 % |
| Variación dinero 2024→2025 | −37.06 % |
| Joins 5.c (filas / huérfanos) | 30 456 / 0 |
| spark-excel | 3.3.2_0.19.0 |

---

## Checklist antes de entrar al aula

- [ ] Cronómetro ensayado una vez completa (≤ 10 min)
- [ ] Luis y Breiner saben en qué diapositiva se pasan la voz
- [ ] Memorizados: mediana vs promedio, los tres jobs de 3.c.3, DIRPOFIS no es ranking operativo
- [ ] PDF de la presentación abierto y en pantalla completa
- [ ] Si preguntan por capturas o código, saben dónde está en la carpeta del zip
