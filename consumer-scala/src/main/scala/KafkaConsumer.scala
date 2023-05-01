package org.example

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaConsumer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaConsumer").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(15))

    val kafkaParams = getKafkaParams()

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array("books"), kafkaParams)
    )

    val sortedWordCounts = getSortedWordCounts(stream)

    sortedWordCounts.foreachRDD { rdd =>
      val top5Words = rdd.take(5)
      if (top5Words.nonEmpty) {
        println("Top 5 words:")
        top5Words.foreach { case (count, word) =>
          println(s"$word: $count")
        }
        val timestamp = System.currentTimeMillis()
        val dateString = new java.text.SimpleDateFormat("dd.MM.yyyy-HH:mm:ss").format(new java.util.Date(timestamp))
        val output = rdd.filter(top5Words.contains).map { case (count, word) =>
          s"$word: $count"
        }
        val outputRepartitioned = output.coalesce(1)
        outputRepartitioned.saveAsTextFile(s"src/main/resources/results_$dateString")
      } else {
        println("No data received in this batch.")
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getKafkaParams(): Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:29092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "book-analytics-consumer-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def getSortedWordCounts(stream: InputDStream[ConsumerRecord[String, String]]): DStream[(Int, String)] = {
    val lines = stream.map(_.value)

    val words = lines.map(_.split("\\W+"))

    val wordStream = words.flatMap(_.iterator)

    val wordCounts = wordStream
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordCounts
      .map(_.swap)
      .transform(rdd => rdd.sortByKey(false))
  }
}
