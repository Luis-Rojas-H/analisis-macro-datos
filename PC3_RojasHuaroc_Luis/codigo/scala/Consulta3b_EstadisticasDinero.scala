// PC3 3.b — Promedio (media aritmetica), media (mediana), desviacion estandar, mayor y menor
// del campo "DINERO INCAUTADO (SOLES)" a nivel nacional, usando MapReduce puro sobre RDD.
//   - Un aggregate acumula suma, suma de cuadrados, conteo, maximo y minimo.
//   - Un sortBy + zipWithIndex obtiene la mediana (n par: promedio de los dos centrales).
//
// Pregunta: ¿cuanto dinero en soles incauta en promedio la PNP por registro, cual es la mediana,
// que tan dispersos son esos montos (desviacion estandar), y cuales son el mayor y el menor monto?
//
// Correr: spark-shell -i scala/Consulta3b_EstadisticasDinero.scala

val raw = sc.textFile("dataset_produccion_policial_2023_2025.csv")
val header = raw.first()
val datos = raw.filter(_ != header)

val valoresDinero = datos.map(_.split(","))
  .filter(c => c(2) == "DINERO INCAUTADO (SOLES)")
  .map(c => c(5).toDouble)
  .cache()

// acumulador = (suma, sumaCuadrados, conteo, maximo, minimo)
type Acum = (Double, Double, Long, Double, Double)

val acumInicial: Acum = (0.0, 0.0, 0L, Double.MinValue, Double.MaxValue)

def combinar(a: Acum, v: Double): Acum =
  (a._1 + v, a._2 + v * v, a._3 + 1L, math.max(a._4, v), math.min(a._5, v))

def fusionar(a: Acum, b: Acum): Acum =
  (a._1 + b._1, a._2 + b._2, a._3 + b._3, math.max(a._4, b._4), math.min(a._5, b._5))

val (suma, sumaCuad, n, maximo, minimo) = valoresDinero.aggregate(acumInicial)(combinar, fusionar)

val promedio = suma / n
val varianza = (sumaCuad / n) - (promedio * promedio)
val desviacion = math.sqrt(varianza)

// Media = mediana: con n par, promedio de los dos valores centrales tras ordenar.
val ordenados = valoresDinero.sortBy(identity).zipWithIndex().map(_.swap)
val mediana = ordenados
  .filter { case (idx, _) => idx == n / 2L - 1L || idx == n / 2L }
  .values
  .mean()

println(f"Campo analizado: DINERO INCAUTADO (SOLES)")
println(f"n (registros)      = $n")
println(f"Promedio (aritm.)  = S/ $promedio%.2f")
println(f"Media (mediana)    = S/ $mediana%.2f")
println(f"Desviacion std.    = S/ $desviacion%.2f")
println(f"Mayor              = S/ $maximo%.2f")
println(f"Menor              = S/ $minimo%.2f")

val salida = Seq(
  s"n\t$n",
  s"promedio\t$promedio",
  s"mediana\t$mediana",
  s"desviacion\t$desviacion",
  s"mayor\t$maximo",
  s"menor\t$minimo"
)
sc.parallelize(salida, 1).saveAsTextFile("../resultados/salida_3b_estadisticas_dinero")

println("CONSULTA_3B_OK")
System.exit(0)
