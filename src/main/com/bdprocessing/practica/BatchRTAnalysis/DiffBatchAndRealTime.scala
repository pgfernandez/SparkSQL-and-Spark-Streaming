package main.com.bdprocessing.practica.BatchRTAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DiffBatchAndRealTime {

  def main(args: Array[String]): Unit = {

    //para recibir vía argumento el umbral de la diferencia entre
    //el consumo en tiempo real y el histórico registrado
    //si no tengo al menos un argumento, no han pasado el umbral

    if (args.length < 1){
      println("Por favor, introduzca el umbral de diferencia de consumo")
      System.exit(1)
    } else {

      //compruebo que es numérico, lo he convertido a entero
      try{
        args(0).toInt

      }catch{
        case n: NumberFormatException => {
          println("Por favor, introduzca un umbral en formato numérico")
          System.exit(1)
        }
      }
    }

    //configuración nivel de log
    Logger.getLogger("org").setLevel(Level.ERROR)

    //creación de sesión
    val sparkSession = SparkSession.builder()
      .appName("ships-fuel-analysis-data")
      .config("spark.master" ,"local[*]")
      .getOrCreate()

    //permite convertir objetos de Scala en Dataframe
    import sparkSession.implicits._

    //lectura del csv de las naves de transporte creando un Dataframe
    val dataBatch = sparkSession.read
      .option("header", "false")
      .option("inferSchema", value = true)
      .csv("in/Gasto_Medio_Batch.csv")

    //lectura del csv de todos los trayectos de naves creando un Dataframe
    val dataRT = sparkSession.read
      .option("header", "false")
      .option("inferSchema", value = true)
      .csv("in/Gasto_Medio_RT.csv")


    //tratamiento
    val dataBatchWithNamedColumns = dataBatch.select($"_c0".alias("Codigo"), $"_c1".alias("Nombre"), $"_c2".alias("GastoMedioBatch"))
    val dataRThWithNamedColumns = dataRT.select($"_c0", $"_c1".alias("GastoMedioRT"))


    //join de las naves de transporte que estén en los trayectos
    //elimino el campo _c1 ya que me aparece duplicado, con Codigo, tras el join
    //calculo la media de gasto de Coltanita agrupado por Codigo de nave
    val analysisConsumption = dataBatchWithNamedColumns.join(dataRThWithNamedColumns, dataBatchWithNamedColumns("Codigo") === dataRThWithNamedColumns("_c0")).drop("_c0")
        .select($"Codigo", $"Nombre", $"GastoMedioBatch", $"GastoMedioRT", $"GastoMedioRT" - $"GastoMedioBatch" as "Diferencia(RT - Batch)")

    println("********** ANÁLISIS GENERAL DEL CONSUMO DE LAS NAVES *********")
    analysisConsumption.show()

    //escribo los resultados en un fichero usando solo una partición
    analysisConsumption.repartition(1).write.csv("out/diffGastoMedio")


    //LAS MEJORES NAVES SEGÚN CONSUMO HISTÓRICO
    //ordeno los resultados por consumo del batch y me quedo sólo con código de nave, su nombre y su gasto en el histórico
    println("********** LISTA DE LAS MEJORES NAVES SEGÚN SU CONSUMO MEDIO HISTÓRICO *********")

    val orderedByHistoricConsumption = analysisConsumption.select($"Codigo", $"Nombre", $"GastoMedioBatch").orderBy("GastoMedioBatch")

    orderedByHistoricConsumption.show()

    println("********** LISTA DE LAS MEJORES NAVES SEGÚN SU CONSUMO MEDIO EN TIEMPO REAL *********")


    //LAS MEJORES NAVES SEGÚN CONSUMO EN TIEMPO REAL
    val orderedByRTConsumption = analysisConsumption.select($"Codigo", $"Nombre", $"GastoMedioRT").orderBy("GastoMedioRT")

    orderedByRTConsumption.show()


    //LAS MEJORES NAVES SEGÚN EL MENOR DIFERENCIAL ENTRE HSITÓRICO Y TIEMPO REAL
    println("********** CONVERSIÓN A LISTA DE LAS MEJORES NAVES SEGÚN SU MENOR DIFERENCIAL ENTRE TIEMPO REAL E HISTÓRICO *********")

    //recojo el umbral
    val umbralConsumo = args(0).toInt
    //ordenadas por menor diferencia y si son menores al umbral pasado
    val orderedByDiffConsumption = analysisConsumption.select($"Codigo", $"Nombre", $"Diferencia(RT - Batch)").filter($"Diferencia(RT - Batch)" < umbralConsumo).orderBy("Diferencia(RT - Batch)")

    orderedByDiffConsumption.show()

    //convierto a lista los resultados y los muestro por consola
    println(orderedByDiffConsumption.takeAsList(3))

    //cierre de sesión
    sparkSession.close()

  }

}
