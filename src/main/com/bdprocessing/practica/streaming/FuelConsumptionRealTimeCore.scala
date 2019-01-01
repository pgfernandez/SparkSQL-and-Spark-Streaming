package main.com.bdprocessing.practica.streaming

import java.io.{File, PrintWriter}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object FuelConsumptionRealTimeCore {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ssc = new StreamingContext("local[*]", "realtime-fuel-consumption",Seconds(30))


    ssc.checkpoint("out/checkpoint")

    //parametros kafka
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    //me subscribo al tópico
    val topics = Array("interstellar")

    //crear el DStream
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent, Subscribe[String,String](topics,kafkaParams))

    //capturamos el valor del mensaje proveniente de kafka
    val shipsRT = stream.map(ship => ship.value)

    //nos quedamos con el campo identificador de la nave y consumo del viaje
    //creo el contador
    val cleanShipsRT = shipsRT.map(shipData => (shipData.split(",")(1).toInt, (1,shipData.split(",")(9).toDouble)))

    //optenemos el acumulado de vuelos y consumo total
    val dataRT = cleanShipsRT.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    //los vamos mostrando
    dataRT.print

    //aquí realizamos el cálculo de la media
    //y escribimos el fichero
    dataRT.foreachRDD((rdd, time) => {

      if (rdd.count() > 0) {

        val rddCached = rdd.cache()

        //escribo el fichero con los gastos medios procesados
        val writer = new PrintWriter(new File("out/gastoMedioRTCore/gastoMedioRTCore.csv"))
        for((shipCode ,(flights, consumption)) <- rddCached.collect()) writer.write(shipCode +"," + consumption / flights + "\n")

        writer.close()
      } else {

        System.exit(0)
      }

    })
    //escucha activa
    ssc.start()
    ssc.awaitTermination()

  }


}
