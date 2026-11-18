// PC3 3.c (2/3) — Porcentaje que representa cada tipo de droga (PBC, Marihuana, CC, en KILOS)
// dentro del total de kilos incautados por region/frente. MapReduce TRIPLE sobre RDD:
//   Job 1: suma de kilos por (region, tipo de droga).
//   Job 2: suma total de kilos por region (reagrupando la salida del job 1).
//   Job 3: porcentaje de cada tipo dentro de su region, ordenado de mayor a menor.
//
// Pregunta: dentro de cada region/frente policial, ¿que droga (PBC, marihuana o clorhidrato
// de cocaina) representa la mayor proporcion del total de kilos incautados?
//
// Correr: spark-shell -i scala/Consulta3c2_PorcentajeDrogaPorRegion.scala

val raw = sc.textFile("dataset_produccion_policial_2023_2025.csv")
val header = raw.first()
val drogasKilos = Set("PBC (KILOS)", "MARIHUANA (KILOS)", "CC (KILOS)")

val regionesValidas = Set("DIRNIC", "DIRNOS", "Frente", "Región")

val datos = raw.filter(_ != header).map(_.split(","))
  .filter(c => drogasKilos.contains(c(2)))
  // corrige las filas de 2025 donde DIR_REG_FRE quedo mal copiado (repite el nombre de
  // la dependencia, ej. "RP ICA"/"RP TUMBES", en vez de "Región")
  .map(c => if (regionesValidas.contains(c(4))) c else c.updated(4, "Región"))

// ---------- Job 1: suma de kilos por (region, droga) ----------
val porRegionDroga = datos
  .map(c => ((c(4), c(2)), c(5).toDouble))
  .reduceByKey(_ + _)
  .cache()

// ---------- Job 2: total de kilos por region (reagrupa la salida del job 1) ----------
val totalPorRegion = porRegionDroga
  .map { case ((region, _), kilos) => (region, kilos) }
  .reduceByKey(_ + _)
  .collectAsMap()

// ---------- Job 3: porcentaje de cada droga dentro de su region ----------
val porcentajes = porRegionDroga
  .map { case ((region, droga), kilos) =>
    val total = totalPorRegion(region)
    val pct = if (total > 0) 100.0 * kilos / total else 0.0
    (region, droga, kilos, pct)
  }
  .sortBy { case (region, _, _, pct) => (region, -pct) }
  .collect()

println(f"${"REGION/FRENTE"}%-15s ${"DROGA"}%-20s ${"KILOS"}%12s ${"% DEL TOTAL"}%12s")
porcentajes.foreach { case (region, droga, kilos, pct) =>
  println(f"$region%-15s $droga%-20s $kilos%12.3f $pct%11.2f%%")
}

val salida = porcentajes.map { case (region, droga, kilos, pct) => s"$region\t$droga\t$kilos\t$pct" }
sc.parallelize(salida, 1).saveAsTextFile("../resultados/salida_3c2_porcentaje_droga_region")

println("CONSULTA_3C2_OK")
System.exit(0)
