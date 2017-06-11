package com.chenjun.spark.KafkaStreaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import org.codehaus.jackson.map.ObjectMapper;
import java.util.*;
import java.util.regex.Pattern;

/**
 * count
 *
 */

public class KafkaWordCount {
    public static void main( String[] args ) throws Exception {
        final Pattern SPACE = Pattern.compile(" ");
        final String NUM_THREAD = "1";
        final String zkQuorum = "localhost:2181";
        final String TOPICS = "action";
        final String group = "1";
        //设置core数为2,threads=2,若出错，修改核心数或许有用
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[4]");
        // Create the context with 1 seconds batch size，一秒的队列批次延时
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
        int numThreads = Integer.parseInt(NUM_THREAD);;
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = TOPICS.split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }
        //创建消费者连接
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group,topicMap);
        //行的形式收入数据流
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });
        //行作flatmap操作，空格划分
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String x) {
                return Arrays.asList(SPACE.split(x)).iterator();
            }
        });
        //Wordcount算法，计数
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        //对Dstream数据流里的每个RDD遍历
        wordCounts.foreachRDD((new VoidFunction2<JavaPairRDD<String, Integer>, Time>(){
            public void call(JavaPairRDD<String, Integer> stringIterator, Time time) throws Exception{
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);//创建生产者
                //map到JSON的转换器
                ObjectMapper mapper = new ObjectMapper();
                //使用collectAsMap()函数将RDD中数据转换为map形式，再通过writeValueAsString转换成json数据格式
                String json=mapper.writeValueAsString(stringIterator.collectAsMap());
                System.out.println(json);//打印json数据
                //创建生产者，json格式数据发送到kafka主题“result”
                producer.send(new ProducerRecord<String, String>("result", "msg", json));
            }
        }));
        jssc.start();
        jssc.awaitTermination();
    }
}
