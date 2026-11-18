// PC3 punto 5.a-5.d — Spark SQL con API de DataFrame (Scala), leyendo las 3 tablas ya
// cargadas en PostgreSQL por el ETL (scala/ETL_CargarDatos.scala).
//
// Correr: spark-shell --driver-class-path postgresql-42.7.6.jar --jars postgresql-42.7.6.jar
//         --driver-java-options "-Dfile.encoding=UTF-8" -i sparksql/5_ConsultasDataFrame.scala

import org.apache.spark.sql.functions._

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

// detalle: hecho + las dos dimensiones, para poder filtrar/agrupar por categoria, unidad,
// dependencia y region a la vez.
val detalleDF = hechoDF
  .join(depDF, "dependencia_id")
  .join(varDF, "variable_id")
  .select(
    hechoDF("anio"), hechoDF("mes"), hechoDF("valor"),
    depDF("nombre").as("dependencia"), depDF("dir_reg_fre"),
    varDF("nombre").as("variable"), varDF("categoria"), varDF("unidad"))
  .cache()

println("=== Filas en detalleDF: " + detalleDF.count() + " ===")

// ================= 5.a — 4 consultas con groupBy + filter, 2+ columnas =================
println("\n--- 5a.1: suma de valor por (anio, categoria), filtrando valor > 0 ---")
val q5a1 = detalleDF.filter($"valor" > 0)
  .groupBy("anio", "categoria")
  .agg(sum("valor").as("suma_valor"))
  .orderBy("anio", "categoria")
q5a1.show(50, false)

println("\n--- 5a.2: conteo de registros por (dependencia, mes), filtrando anio = 2024 ---")
val q5a2 = detalleDF.filter($"anio" === 2024)
  .groupBy("dependencia", "mes")
  .agg(count("*").as("n_registros"))
  .orderBy(desc("n_registros"))
q5a2.show(20, false)

println("\n--- 5a.3: suma de kilos por (dir_reg_fre, categoria), filtrando unidad = 'Kilos' ---")
val q5a3 = detalleDF.filter($"unidad" === "Kilos")
  .groupBy("dir_reg_fre", "categoria")
  .agg(sum("valor").as("kilos"))
  .orderBy(desc("kilos"))
q5a3.show(20, false)

println("\n--- 5a.4: total por (anio, unidad), filtrando categoria = 'Droga' ---")
val q5a4 = detalleDF.filter($"categoria" === "Droga")
  .groupBy("anio", "unidad")
  .agg(sum("valor").as("total"))
  .orderBy("anio", "unidad")
q5a4.show(20, false)

// ================= 5.b — 1 promedio + 1 orderBy desc =================
println("\n--- 5b.1: promedio de valor por categoria ---")
val q5b1 = detalleDF.groupBy("categoria")
  .agg(avg("valor").as("promedio_valor"))
  .orderBy("categoria")
q5b1.show(20, false)

println("\n--- 5b.2: dependencias ordenadas por suma total de valor, descendente ---")
val q5b2 = detalleDF.groupBy("dependencia")
  .agg(sum("valor").as("total_valor"))
  .orderBy(desc("total_valor"))
q5b2.show(20, false)

// ================= 5.c — 4 joins, uno de cada tipo =================
println("\n--- 5c.1: INNER join hecho x dim_variable ---")
val q5c1 = hechoDF.join(varDF, "variable_id")
  .select("anio", "mes", "variable_id", "nombre", "categoria", "valor")
println("filas inner: " + q5c1.count())
q5c1.show(5, false)

println("\n--- 5c.2: LEFT OUTER join dim_dependencia x hecho (detecta dependencias sin registros) ---")
val q5c2 = depDF.join(hechoDF, Seq("dependencia_id"), "left_outer")
println("filas left_outer: " + q5c2.count())
val sinRegistros = q5c2.filter($"id".isNull)
println("dependencias sin ningun registro: " + sinRegistros.count())

println("\n--- 5c.3: RIGHT OUTER join hecho x dim_dependencia ---")
val q5c3 = hechoDF.join(depDF, Seq("dependencia_id"), "right_outer")
println("filas right_outer: " + q5c3.count())

println("\n--- 5c.4: FULL OUTER join dim_variable x hecho (detecta variables sin uso) ---")
val q5c4 = varDF.join(hechoDF, Seq("variable_id"), "full_outer")
println("filas full_outer: " + q5c4.count())
val variablesSinUso = q5c4.filter($"id".isNull)
println("variables sin ningun registro: " + variablesSinUso.count())

// ================= 5.d — 4 consultas con org.apache.spark.sql.functions._ =================
println("\n--- 5d.1: round(avg(valor), 2) por categoria ---")
val q5d1 = detalleDF.groupBy("categoria")
  .agg(round(avg("valor"), 2).as("promedio_redondeado"))
  .orderBy("categoria")
q5d1.show(20, false)

println("\n--- 5d.2: clasificacion de valor en rangos con when/otherwise ---")
val q5d2 = detalleDF.withColumn("rango_valor",
    when($"valor" === 0, "sin incidencia")
      .when($"valor" < 100, "bajo")
      .when($"valor" < 10000, "medio")
      .otherwise("alto"))
  .groupBy("rango_valor").count()
  .orderBy("rango_valor")
q5d2.show(20, false)

println("\n--- 5d.3: etiqueta con concat_ws(dependencia - mes - anio) ---")
val q5d3 = detalleDF
  .withColumn("etiqueta", concat_ws(" - ", $"dependencia", $"mes", $"anio"))
  .select("etiqueta", "variable", "valor")
q5d3.show(10, false)

println("\n--- 5d.4: desviacion estandar (stddev) de valor por categoria ---")
val q5d4 = detalleDF.groupBy("categoria")
  .agg(round(stddev("valor"), 2).as("desviacion_std"))
  .orderBy("categoria")
q5d4.show(20, false)

println("CONSULTAS_5A_5D_OK")
System.exit(0)
