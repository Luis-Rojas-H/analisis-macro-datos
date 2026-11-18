// PC3 CC531 — Proceso ETL: Extraer el CSV de PNDA, Transformar (limpieza + normalizacion
// a esquema en estrella) y Cargar a PostgreSQL.
//
// Fuente: "Resultados de las operaciones policiales a nivel nacional [MININTER]" (CENOPPOL),
// anios 2023-2025, descargado de datosabiertos.gob.pe.
//
// Correr con (desde la carpeta PC3Macro):
// spark-shell --driver-class-path postgresql-42.7.6.jar --jars postgresql-42.7.6.jar -i scala/ETL_CargarDatos.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode

val urlJdbc = "jdbc:postgresql://localhost:5432/postgres"
val propsUser = "postgres"
val propsPass = "postgres123"

def guardar(df: org.apache.spark.sql.DataFrame, tabla: String): Unit = {
  df.write.format("jdbc")
    .option("url", urlJdbc).option("dbtable", tabla)
    .option("user", propsUser).option("password", propsPass)
    .mode(SaveMode.Append)
    .save()
}

// ---------- EXTRACT ----------
// El encabezado original tiene "AÑO" con enie; para evitar problemas de codificacion al
// escribir ese literal en un .scala leido en Windows, se renombran las columnas por
// posicion justo despues de leer (orden real del CSV: AÑO,MES,VARIABLE,DEPENDENCIAS,
// DIR_REG_FRE,VALOR).
val crudoRaw = spark.read
  .option("header", "true")
  .option("encoding", "UTF-8")
  .csv("dataset_produccion_policial_2023_2025.csv")

val crudoDF = crudoRaw.toDF("ANIO_RAW", "MES_RAW", "VARIABLE_RAW", "DEPENDENCIAS_RAW", "DIR_REG_FRE_RAW", "VALOR_RAW")

println("Filas leidas del CSV: " + crudoDF.count())

// ---------- TRANSFORM ----------
// 1) Corrige las 432 filas de 2025 donde DIR_REG_FRE quedo mal copiado (repite el nombre
//    de la dependencia -- "RP ICA" / "RP TUMBES" -- en vez de "Region"). Es un problema de
//    calidad de datos real de la fuente, documentado en el informe.
val dirRegFreCorregido = when(
  col("DIR_REG_FRE_RAW").isin("DIRNIC", "DIRNOS", "Frente", "Región"), col("DIR_REG_FRE_RAW")
).otherwise(lit("Región"))

// 2) Deriva categoria y unidad a partir del texto de VARIABLE (18 valores distintos en el
//    dataset original), para poder construir una dimension significativa.
val categoriaExpr = col("VARIABLE_RAW")
val categoriaCalc = when(categoriaExpr.contains("ARMAS"), "Arma")
  .when(categoriaExpr.contains("BANDAS") || categoriaExpr.contains("ORGANIZACIONES"), "Organización criminal")
  .when(categoriaExpr.rlike("^(CC|MARIHUANA|PBC)"), "Droga")
  .when(categoriaExpr.contains("CONTRABANDO"), "Contrabando")
  .when(categoriaExpr.contains("DETENIDOS") || categoriaExpr.contains("REQUISITORIADO"), "Detenido")
  .when(categoriaExpr.contains("DINERO"), "Dinero")
  .when(categoriaExpr.contains("INTERVENCION"), "Migración")
  .when(categoriaExpr.contains("OPERATIVOS"), "Operativo")
  .when(categoriaExpr.contains("PAPELETAS"), "Infracción de tránsito")
  .otherwise("Otro")

val unidadCalc = when(categoriaExpr.contains("KILOS"), "Kilos")
  .when(categoriaExpr.contains("ENVOLTORIO"), "Envoltorios")
  .when(categoriaExpr.contains("SOLES") || categoriaExpr.contains("VALORIZACION"), "Soles")
  .when(categoriaExpr.contains("DETENIDOS") || categoriaExpr.contains("REQUISITORIADO") || categoriaExpr.contains("INTERVENCION"), "Personas")
  .otherwise("Unidades")

val limpioDF = crudoDF
  .withColumn("anio", col("ANIO_RAW").cast("int"))
  .withColumn("mes", col("MES_RAW"))
  .withColumn("dependencia", trim(col("DEPENDENCIAS_RAW")))
  .withColumn("dir_reg_fre", dirRegFreCorregido)
  .withColumn("variable", trim(col("VARIABLE_RAW")))
  .withColumn("categoria", categoriaCalc)
  .withColumn("unidad", unidadCalc)
  .withColumn("valor", col("VALOR_RAW").cast("decimal(14,3)"))
  .select("anio", "mes", "dependencia", "dir_reg_fre", "variable", "categoria", "unidad", "valor")
  .cache()

println("Filas tras limpieza: " + limpioDF.count())

// 3) Dimension de dependencias, con llave subrogada asignada en Spark (orden determinista).
val wDep = Window.orderBy("dependencia")
val dimDependenciaDF = limpioDF
  .select("dependencia", "dir_reg_fre").distinct()
  .withColumn("dependencia_id", row_number().over(wDep))
  .select(col("dependencia_id"), col("dependencia").as("nombre"), col("dir_reg_fre"))
  .cache()
println("Dependencias distintas: " + dimDependenciaDF.count())

// 4) Dimension de variables, con llave subrogada asignada en Spark.
val wVar = Window.orderBy("variable")
val dimVariableDF = limpioDF
  .select("variable", "categoria", "unidad").distinct()
  .withColumn("variable_id", row_number().over(wVar))
  .select(col("variable_id"), col("variable").as("nombre"), col("categoria"), col("unidad"))
  .cache()
println("Variables distintas: " + dimVariableDF.count())

// 5) Tabla de hechos: se arma uniendo el detalle limpio con las dos dimensiones ya
//    calculadas, para quedarse solo con las llaves foraneas.
val hechoDF = limpioDF
  .join(dimDependenciaDF, limpioDF("dependencia") === dimDependenciaDF("nombre"))
  .join(dimVariableDF, limpioDF("variable") === dimVariableDF("nombre"))
  .select(
    limpioDF("anio"), limpioDF("mes"),
    dimDependenciaDF("dependencia_id"), dimVariableDF("variable_id"),
    limpioDF("valor"))
  .cache()
println("Filas de hecho: " + hechoDF.count())

// ---------- LOAD ----------
guardar(dimDependenciaDF.select("dependencia_id", "nombre", "dir_reg_fre"), "dim_dependencia")
guardar(dimVariableDF.select("variable_id", "nombre", "categoria", "unidad"), "dim_variable")
guardar(hechoDF, "hecho_resultado_operativo")

println("ETL_OK")
System.exit(0)
