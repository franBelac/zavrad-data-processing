package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("KafkaConsumer").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(15));

        Map<String, Object> kafkaParams = getKafkaParams();

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singleton("books"), kafkaParams)
        );

        var sortedWordCounts = getSortedWordCounts(stream);

        sortedWordCounts.foreachRDD(rdd -> {
            List<Tuple2<Integer, String>> top5Words = rdd.take(5);
            if (!top5Words.isEmpty()) {
                System.out.println("Top 5 words:");
                for (Tuple2<Integer, String> wordCount : top5Words) {
                    System.out.println(wordCount._2() + ": " + wordCount._1());
                }
                long timestamp = System.currentTimeMillis();
                SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy-HH:mm:ss");
                String dateString = sdf.format(new Date(timestamp));
                JavaRDD<String> output = rdd.filter(top5Words::contains).map(tuple -> tuple._2() + ": " + tuple._1);
                JavaRDD<String> outputRepartitioned = output.coalesce(1);
                outputRepartitioned.saveAsTextFile("src/main/resources/results_" + dateString);
            } else {
                System.out.println("No data received in this batch.");
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    private static Map<String,Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:29092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "book-analytics-consumer-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    private static JavaPairDStream<Integer, String> getSortedWordCounts(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);

        JavaDStream<String[]> words = lines.map(line -> line.split("\\W+"));

        JavaDStream<String> wordStream = words
                .flatMap((FlatMapFunction<String[], String>) strings -> Arrays.asList(strings).iterator());

        JavaPairDStream<String, Integer> wordCounts = wordStream
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        return wordCounts
                .mapToPair(Tuple2::swap)
                .transformToPair(rdd -> rdd.sortByKey(false));
    }

}