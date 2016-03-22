package controllers;

import play.*;
import play.mvc.*;

//import kafka.*;
//import models.*;
import java.util.Properties;
import java.util.*;
import kafka.Kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.*;
import kafka.producer.*;
import kafka.producer.ByteArrayPartitioner;
import kafka.javaapi.producer.Producer;
import static kafka.tools.StateChangeLogMerger.topic;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class Application extends Controller {


  
     
 
    public static void index(){
        render();
    }
    
    public static void sendMessage(String newTitle, String newContent) {
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9093");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("broker.list", "localhost:9092" );
        
        play.Logger.info("create config");
        ProducerConfig config = new ProducerConfig(props);
        
        play.Logger.info("create producer");
        Producer<String, String> producer = new Producer<String, String>(config);

        play.Logger.info("yay!");
        
        KeyedMessage msg = new KeyedMessage<String,String> ("helloTopic", "Greetings!");
       // for (int i = 0; i < 5;i++ ) {
            KeyedMessage<String, String> data = new KeyedMessage<String,String>("helloTopic","Hello", newContent);
            producer.send(data);
       // }
        
        render();
    }
    
    public static ConsumerConfig createConsumerConfig() { 
      Properties props = new Properties(); 
      props.put("zookeeper.connect", "localhost:2181"); 
      props.put("group.id", "test-consumer-group"); 
      props.put("zookeeper.session.timeout.ms", "4000"); 
      props.put("zookeeper.sync.time.ms", "2000"); 
      props.put("auto.commit.interval.ms", "1000"); 
      props.put("auto.offset.reset", "smallest");
      return new ConsumerConfig(props); 
      
    } 

     public static void display() { 
            String msg = null;
              ConsumerConnector consumer = 
            kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
              Map<String, Integer> topicCountMap = new HashMap<String, Integer>(); 
              topicCountMap.put("helloTopic", new Integer(1)); 
              Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap); 
              KafkaStream<byte[], byte[]> stream = consumerMap.get("helloTopic").get(0);
             // List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(helloTopic);
              ConsumerIterator<byte[], byte[]> it = stream.iterator(); 
             // for (int i = 0; i < 5; i++)
              //while(it.hasNext())
              //{
                  
                  msg = new String(it.next().message()) + " ";
                  System.out.println(msg);
                  
             // }
              consumer.shutdown();
             render(msg);
    }
}
    
