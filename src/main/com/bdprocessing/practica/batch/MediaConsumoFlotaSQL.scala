package main.com.bdprocessing.practica.batch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MediaConsumoFlotaSQL {

  def main(args: Array[String]): Unit = {


    //configuración nivel de log
    Logger.getLogger("org").setLevel(Level.ERROR)

    //creación de sesión
    val sparkSession = SparkSession.builder()
      .appName("ships-fuel-data")
      .config("spark.master" ,"local[*]")
      .getOrCreate()

    //permite convertir objetos de Scala en Dataframe
    import sparkSession.implicits._

    //lectura del csv de las naves de transporte creando un Dataframe
    val shipsTransports = sparkSession.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/naves_transporte.csv")

    //lectura del csv de todos los trayectos de naves creando un Dataframe
    val shipsTrips = sparkSession.read
      .option("header", "false")
      .option("inferSchema", value = true)
      .csv("in/trayectos.csv")


    //elimina los duplicados en base al campo Codigo, como no hay criterio para
    //ver con cuál quedarnos , se queda con el primero de los duplicados que encuentre
    val shipsCleaned = shipsTransports.dropDuplicates("Codigo")

    //selecciono el campo de identificador de la nave y del consumo de coltanita
    // le pongo un alias al campo de gasto de coltanita, al identificador no
    //porque no lo necesitaré tras el join y lo eliminaré
    val tripsCleaned = shipsTrips.select($"_c1", $"_c9".alias("GastoColtanita"))


    //join de las naves de transporte que estén en los trayectos
    //elimino el campo _c1 ya que me aparece duplicado, con Codigo, tras el join
    //calculo la media de gasto de Coltanita agrupado por Codigo de nave
    val mediaGasto = shipsCleaned.join(tripsCleaned, shipsCleaned("Codigo") === tripsCleaned("_c1")).drop("_c1")
        .groupBy("Codigo", "Descripcion").avg("GastoColtanita").orderBy("Codigo")

    mediaGasto.show()


    //guardo el csv, en este caso en una partición para poder tenerlo a mano
    //para la comparativa con los de tiempo real

    mediaGasto.repartition(1).write.csv("out/gastoMedioSQL")

    //cierre de sesión
    sparkSession.close()


  }

}
