package main.com.bdprocessing.practica.batch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.io.PrintWriter


object MediaConsumoFlotaRDD {

  def main(args: Array[String]): Unit = {

    //configuración nivel de log
    Logger.getLogger("org").setLevel(Level.ERROR)

    //configuración Spark
    val conf = new SparkConf().setAppName("intestellar-data").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //ingesta de datos
    val shipsRaw = sc.textFile( "in/naves_transporte.csv")
    val shipsTrajectories = sc.textFile( "in/trayectos.csv")

    //PROCESO DE LIMPIEZA

    /* Elimino la cabecera del csv
       elimino el " del inicio y fin del código y del nombre
      elimino los id duplicados quedándome con el primero que encuentre */
    val shipsCleaned = shipsRaw.filter(!_.contains("Codigo"))
      .map(shipTuple => (shipTuple.split(",")(0).drop(1).dropRight(1).toInt, shipTuple.split(",")(1).drop(1).dropRight(1)))
      .reduceByKey((k1,k2) => k1)


    //selecciono sólo los datos, campos, que se necesitan para el procesamiento
    //código de nave que es la clave y e valor es otra tupla con el número de ocurrencias (viajes)
    //que sale en el fichero y el consumo total de todos sus viajes
    val cleanShipsTrajectories = shipsTrajectories.map(linea => (linea.split(",")(1).toInt,
      (1,linea.split(",")(9).toDouble)))

    //suma de número de vuelos y gasto total de Coltanita
    val totalShipFuelConsumption = cleanShipsTrajectories.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) //estoy sumando el contador (el 1 de lo que estaba en la tupla y luego sumando los precios

    //Join de las naves de transporte que tengan trayectos
    //me las ordenas por el id de las naves de transporte
    val shipsTransportsTrips = shipsCleaned.leftOuterJoin(totalShipFuelConsumption).sortByKey()


    //PROCESAMIENTO DE DATOS

    //número de vuelos totales de cada nave de transporte y su gasto total de Coltanita
    println("******* SHIP'S NUMBER OF FLIGHTS AND TOTAL COLTANITE CONSUMPTION ********")

    for ((shipCode, (name, consumption)) <- shipsTransportsTrips.collect()) println("Ship: (" + shipCode + ") " + name + " flew " +  consumption.head._1  + " space trips, consuming a total of " + consumption.head._2 + " of Coltanite") //println("******* SHIP'S NUMBER OF FLIGHTS AND TOTAL COLTANITE CONSUMPTION ********")

    //media de consumo de Coltanita por nave
    println("******* SHIP'S COLTANITE MEAN CONSUMPTION *********")
    for((shipCode ,(name, consumption)) <- shipsTransportsTrips.collect()) println("Ship: (" + shipCode + ") " + name + " ->" + consumption.head._2 / consumption.head._1 + " of Coltanite")

    //escribo el fichero con los gastos medios procesados
    val writer = new PrintWriter(new File("out/gastoMedioCore/Gasto_Medio_Batch_Core.csv"))
    for((shipCode ,(name, consumption)) <- shipsTransportsTrips.collect()) writer.write(shipCode +"," + name + "," + consumption.head._2 / consumption.head._1 + "\n")

    writer.close()

  }

}
