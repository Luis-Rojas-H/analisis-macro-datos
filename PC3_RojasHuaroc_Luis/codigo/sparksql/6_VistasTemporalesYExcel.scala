// PC3 punto 5.e-5.g — Vistas temporales + Spark SQL (sentencias SQL puras), y exportacion de
// los resultados a Excel usando Spark.
//
// Correr: spark-shell --driver-class-path postgresql-42.7.6.jar --jars postgresql-42.7.6.jar
//         --packages com.crealytics:spark-excel_2.12:3.3.2_0.19.0
//         --driver-java-options "-Dfile.encoding=UTF-8" -i sparksql/6_VistasTemporalesYExcel.scala

val urlJdbc = "jdbc:postgresql://localhost:5432/postgres"
val propsUser = "postgres"
val propsPass = "postgres123"

def leer(tabla: String) = spark.read.format("jdbc")
  .option("url", urlJdbc).option("dbtable", tabla)
  .option("user", propsUser).option("password", propsPass)
  .load()

val hechoDF = leer("hecho_resultado_operativo")
val depDF = leer("dim_dependencia")
val varDF = leer("dim_variable")

hechoDF.createOrReplaceTempView("hecho")
depDF.createOrReplaceTempView("dependencia")
varDF.createOrReplaceTempView("variable")

// ================= 5.e — 3 SELECT con JOIN (objetivos distintos a los de 5.c) =================
println("\n--- 5e.1: INNER JOIN hecho+variable, solo categoria Dinero, por dependencia ---")
val q5e1 = spark.sql("""
  SELECT d.nombre AS dependencia, h.anio, SUM(h.valor) AS dinero_incautado
  FROM hecho h
  INNER JOIN variable v ON h.variable_id = v.variable_id
  INNER JOIN dependencia d ON h.dependencia_id = d.dependencia_id
  WHERE v.categoria = 'Dinero'
  GROUP BY d.nombre, h.anio
  ORDER BY dinero_incautado DESC
""")
q5e1.show(15, false)

println("\n--- 5e.2: LEFT JOIN dependencia+hecho, conteo total de OPERATIVOS por dependencia ---")
val q5e2 = spark.sql("""
  SELECT d.nombre AS dependencia, d.dir_reg_fre,
         COALESCE(SUM(CASE WHEN v.nombre = 'OPERATIVOS' THEN h.valor ELSE 0 END), 0) AS total_operativos
  FROM dependencia d
  LEFT JOIN hecho h ON d.dependencia_id = h.dependencia_id
  LEFT JOIN variable v ON h.variable_id = v.variable_id
  GROUP BY d.nombre, d.dir_reg_fre
  ORDER BY total_operativos DESC
""")
q5e2.show(15, false)

println("\n--- 5e.3: FULL JOIN variable+hecho, variables sin registros en 2025 ---")
val q5e3 = spark.sql("""
  SELECT v.nombre AS variable, v.categoria, COUNT(h.id) AS n_registros_2025
  FROM variable v
  FULL JOIN hecho h ON v.variable_id = h.variable_id AND h.anio = 2025
  GROUP BY v.nombre, v.categoria
  ORDER BY n_registros_2025 ASC
""")
q5e3.show(20, false)

// ================= 5.f — 3 ORDER BY combinados con filtros =================
println("\n--- 5f.1: ARMAS, mayor a menor ---")
val q5f1 = spark.sql("""
  SELECT d.nombre AS dependencia, h.anio, h.mes, v.nombre AS tipo_arma, h.valor
  FROM hecho h
  JOIN variable v ON h.variable_id = v.variable_id
  JOIN dependencia d ON h.dependencia_id = d.dependencia_id
  WHERE v.categoria = 'Arma'
  ORDER BY h.valor DESC
  LIMIT 10
""")
q5f1.show(10, false)

println("\n--- 5f.2: Region (no Frente/DIRNIC/DIRNOS), anio 2025, mayor a menor ---")
val q5f2 = spark.sql("""
  SELECT d.nombre AS dependencia, v.nombre AS variable, h.valor
  FROM hecho h
  JOIN dependencia d ON h.dependencia_id = d.dependencia_id
  JOIN variable v ON h.variable_id = v.variable_id
  WHERE h.anio = 2025 AND d.dir_reg_fre = 'Región'
  ORDER BY h.valor DESC
  LIMIT 10
""")
q5f2.show(10, false)

println("\n--- 5f.3: dinero incautado > 0, menor a mayor ---")
val q5f3 = spark.sql("""
  SELECT d.nombre AS dependencia, h.anio, h.mes, h.valor AS soles_incautados
  FROM hecho h
  JOIN variable v ON h.variable_id = v.variable_id
  JOIN dependencia d ON h.dependencia_id = d.dependencia_id
  WHERE v.unidad = 'Soles' AND h.valor > 0
  ORDER BY h.valor ASC
  LIMIT 10
""")
q5f3.show(10, false)

// ================= 5.g — Guardar e y f en Excel usando Spark =================
// La primera hoja de cada archivo usa "overwrite" (crea el archivo); las siguientes usan
// "append", que en spark-excel agrega una hoja nueva al mismo libro en vez de borrar lo ya
// escrito.
def aExcel(df: org.apache.spark.sql.DataFrame, hoja: String, ruta: String, primera: Boolean): Unit = {
  df.coalesce(1).write
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", s"'$hoja'!A1")
    .mode(if (primera) "overwrite" else "append")
    .save(ruta)
}

aExcel(q5e1, "5e1_dinero_por_dependencia", "../excel/PC3_resultados_5e.xlsx", true)
aExcel(q5e2, "5e2_operativos_por_dependencia", "../excel/PC3_resultados_5e.xlsx", false)
aExcel(q5e3, "5e3_variables_sin_uso_2025", "../excel/PC3_resultados_5e.xlsx", false)
aExcel(q5f1, "5f1_armas_top10", "../excel/PC3_resultados_5f.xlsx", true)
aExcel(q5f2, "5f2_region_2025_top10", "../excel/PC3_resultados_5f.xlsx", false)
aExcel(q5f3, "5f3_dinero_menor_a_mayor", "../excel/PC3_resultados_5f.xlsx", false)

println("CONSULTAS_5E_5F_5G_OK")
System.exit(0)
