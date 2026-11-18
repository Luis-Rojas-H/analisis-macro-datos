// PC3 3.c (3/3) — Tasa de variacion interanual del promedio mensual de dinero incautado
// (SOLES) a nivel nacional, 2023->2024->2025. MapReduce TRIPLE sobre RDD:
//   Job 1: suma de DINERO INCAUTADO y conteo de registros, por anio.
//   Job 2: promedio mensual por anio (suma del anio / 12 meses), ordenado con sortByKey.
//   Job 3: % de variacion de cada anio respecto al anio anterior, con join del RDD
//          de promedios contra si mismo (clave desplazada al anio siguiente).
//
// Pregunta: ¿crecio o disminuyo, anio a anio, el promedio mensual de dinero incautado por la
// PNP a nivel nacional entre 2023 y 2025?
//
// Correr: spark-shell -i scala/Consulta3c3_VariacionInteranualDinero.scala

val raw = sc.textFile("dataset_produccion_policial_2023_2025.csv")
val header = raw.first()
val datos = raw.filter(_ != header).map(_.split(","))
  .filter(c => c(2) == "DINERO INCAUTADO (SOLES)")

// ---------- Job 1: suma y conteo por anio ----------
val porAnio = datos
  .map(c => (c(0).toInt, (c(5).toDouble, 1L)))
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .cache()

// ---------- Job 2: promedio mensual por anio (suma del anio / 12); sortByKey es el shuffle ----------
val promedioMensual = porAnio
  .map { case (anio, (suma, n)) => (anio, (suma / 12.0, suma, n)) }
  .sortByKey()
  .cache()

// ---------- Job 3: join del promedio del anio A (clave = A+1) con el del anio B ----------
val anioAnterior = promedioMensual.map { case (anio, (prom, _, _)) =>
  (anio + 1, (anio, prom))
}
val variaciones = anioAnterior
  .join(promedioMensual.map { case (anio, (prom, _, _)) => (anio, prom) })
  .map { case (anioB, ((anioA, promA), promB)) =>
    val variacionPct = if (promA != 0) 100.0 * (promB - promA) / promA else 0.0
    (anioA, anioB, promA, promB, variacionPct)
  }
  .sortBy(_._1)

// Una sola accion final: materializa promedios y variaciones ya ordenados
val promsArr = promedioMensual.collect()
val varsArr = variaciones.collect()

println(f"${"ANIO"}%6s ${"SUMA TOTAL"}%16s ${"PROM.MENSUAL"}%16s ${"N REGISTROS"}%12s")
promsArr.foreach { case (anio, (promMes, suma, n)) =>
  println(f"$anio%6d $suma%16.2f $promMes%16.2f $n%12d")
}

println(f"\n${"DE"}%6s ${"A"}%6s ${"PROM.A"}%14s ${"PROM.B"}%14s ${"% VARIACION"}%14s")
varsArr.foreach { case (a, b, pa, pb, v) =>
  println(f"$a%6d $b%6d $pa%14.2f $pb%14.2f $v%13.2f%%")
}

val salida = promsArr.map { case (anio, (promMes, suma, n)) => s"PROMEDIO\t$anio\t$promMes\t$suma\t$n" } ++
  varsArr.map { case (a, b, _, _, v) => s"VARIACION\t$a-$b\t$v" }
sc.parallelize(salida, 1).saveAsTextFile("../resultados/salida_3c3_variacion_dinero")

println("CONSULTA_3C3_OK")
System.exit(0)
