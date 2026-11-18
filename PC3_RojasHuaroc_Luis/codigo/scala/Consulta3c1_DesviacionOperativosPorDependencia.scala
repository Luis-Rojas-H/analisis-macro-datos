// PC3 3.c (1/3) — Desviacion del promedio de OPERATIVOS por dependencia respecto al
// promedio nacional. MapReduce TRIPLE sobre RDD:
//   Job 1: suma de OPERATIVOS y conteo de registros, por dependencia.
//   Job 2: promedio nacional (un solo valor, recogido en el driver con collect()).
//   Job 3: para cada dependencia, promedio propio - promedio nacional, ordenado por |desviacion|.
//
// Pregunta: ¿que dependencias policiales reportan muchos mas (o muchos menos) operativos de lo
// normal, comparadas con el promedio nacional de operativos por registro?
//
// Correr: spark-shell -i scala/Consulta3c1_DesviacionOperativosPorDependencia.scala

val raw = sc.textFile("dataset_produccion_policial_2023_2025.csv")
val header = raw.first()
val datos = raw.filter(_ != header).map(_.split(","))
  .filter(c => c(2) == "OPERATIVOS")

// ---------- Job 1: suma y conteo por dependencia ----------
val porDependencia = datos
  .map(c => (c(3), (c(5).toDouble, 1L)))
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .cache()

// ---------- Job 2: promedio nacional ----------
val (sumaNacional, conteoNacional) = porDependencia
  .map(_._2)
  .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
val promedioNacional = sumaNacional / conteoNacional
println(f"Promedio nacional de OPERATIVOS por registro = $promedioNacional%.4f (n=$conteoNacional)")

// ---------- Job 3: desviacion por dependencia, ordenada por |desviacion| desc ----------
val desviaciones = porDependencia
  .map { case (dep, (suma, n)) =>
    val prom = suma / n
    (dep, prom, prom - promedioNacional, n)
  }
  .sortBy { case (_, _, desv, _) => -math.abs(desv) }
  .collect()

println(f"${"DEPENDENCIA"}%-20s ${"PROM.PROPIO"}%12s ${"DESVIACION"}%12s ${"N"}%6s")
desviaciones.foreach { case (dep, prom, desv, n) =>
  println(f"$dep%-20s $prom%12.4f $desv%12.4f $n%6d")
}

val salida = desviaciones.map { case (dep, prom, desv, n) => s"$dep\t$prom\t$desv\t$n" }
sc.parallelize(salida, 1).saveAsTextFile("../resultados/salida_3c1_desviacion_operativos")

println("CONSULTA_3C1_OK")
System.exit(0)
