package com.chenjun.spark.KafkaStreaming;

import java.io.*;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * producer
 *
 */
public class producer {
    public static void main( String[] args ) throws IOException, InterruptedException {

        //创建主题名
        String topicName = "action";
        //创建实例接收生产者属性
        Properties props = new Properties();
        //分配本地端口
        props.put("bootstrap.servers", "localhost:9092");
        //设置生产者请求确认
        props.put("acks", "all");
        //如果请求失败，生产者可以自动重试
        props.put("retries", 0);
        //在配置中指定缓冲区大小
        //发送的时候，可以批量发送的数据量 16K
        props.put("batch.size", 16384);
        //最长等多长时间，批量发送
        props.put("linger.ms", 1);
        //控制可用内存量
        //队列的最大长度 32M
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        while(true) {
            //读取本地CSV文件
            String pathname = "/home/hadoop/SparkStreamingkafkajava/data/user_log.csv";
            File filename = new File(pathname);
            InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
            BufferedReader br = new BufferedReader(reader);
            String line = "";
            line = br.readLine();//去掉表头
            while (line != null) {
                line = br.readLine();
                String[] segments = line.split("\t");//换行符分割
                String[] segment=segments[0].split(",");//逗号分割单词
                System.out.println(segment[7]);//打印购物行为，位于第7个位置
                Thread.sleep(100);//延迟0.1秒发送一个
            //生产者发送消息到kafka主题
            producer.send(new ProducerRecord<String, String>(topicName, "msg", segment[7]));
        }
    }
    }
}
