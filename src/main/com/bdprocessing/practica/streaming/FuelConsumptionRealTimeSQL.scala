package main.com.bdprocessing.practica.streaming


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object FuelConsumptionRealTimeSQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ssc = new StreamingContext("local[*]", "realtime-fuel-consumption",Seconds(20))


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
    val cleanShipsRT = shipsRT.map(shipData => (shipData.split(",")(1).toInt, shipData.split(",")(9).toDouble))

    //para cada RDD lo procesamos
    cleanShipsRT.foreachRDD((rdd, time) => {

      if (rdd.count() > 0) {

        //creación de sesión
        val sparkSession = SparkSession.builder()
          .appName("ships-fuel-data")
          .config("spark.master", "local[*]")
          .getOrCreate()

        //permite convertir objetos de Scala en Dataframe
        import sparkSession.implicits._

        //optimizamos
        val rddCached = rdd.cache()

        //convertimos el RDD en un DataFrame
        val shipDF = rddCached.toDF("Codigo", "GastoColtanita").groupBy("Codigo").avg("GastoColtanita").orderBy("Codigo")
        shipDF.show()

        //escribimos el fichero
        shipDF.repartition(1).write.csv("out/gastoMedioRT_" + time.milliseconds.toString)

      } else {

        System.exit(0)
      }

    })


    //escucha activa
    ssc.start()
    ssc.awaitTermination()

  }


}
