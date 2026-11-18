// PC3 3.a — Agrupar VALOR por VARIABLE (tipo) y encontrar el mayor y el menor de cada tipo,
// usando MapReduce puro sobre RDD (map + reduceByKey).
//
// Pregunta: ¿cual es el mayor y el menor valor registrado historicamente (2023-2025) para
// cada uno de los 18 tipos de resultado operativo policial?
//
// Correr: spark-shell -i scala/Consulta3a_MaxMinPorTipo.scala

val raw = sc.textFile("dataset_produccion_policial_2023_2025.csv")
val header = raw.first()
val datos = raw.filter(_ != header)

// (VARIABLE, VALOR)
val pares = datos.map(_.split(",")).map(c => (c(2), c(5).toDouble))

val maxPorTipo = pares.reduceByKey((a, b) => math.max(a, b))
val minPorTipo = pares.reduceByKey((a, b) => math.min(a, b))

val resultado = maxPorTipo.join(minPorTipo).sortByKey()

println(f"${"VARIABLE"}%-45s ${"MAX"}%12s ${"MIN"}%12s")
resultado.collect().foreach { case (variable, (mx, mn)) =>
  println(f"$variable%-45s $mx%12.3f $mn%12.3f")
}

resultado.map { case (variable, (mx, mn)) => s"$variable\t$mx\t$mn" }
  .coalesce(1)
  .saveAsTextFile("../resultados/salida_3a_maxmin_por_tipo")

println("CONSULTA_3A_OK")
System.exit(0)
