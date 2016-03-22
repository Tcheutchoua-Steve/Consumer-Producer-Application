/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package controllers;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import play.mvc.Controller;

/**
 *
 * @author root
 */
public class Consumer extends Controller{
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
    private String dispMessage = "Empty";
 
    public Consumer(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(  
                createConsumerConfig(a_zookeeper, a_groupId));
        // consumer.createMessageStreams is how we pass this information to kafka
        this.topic = a_topic;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }
 
    public String run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        
        //create a Map that tells Kafka how many threads we are providing for which topics
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        /*kafka uses a single topic for now
          we could have asked for multiple by adding another element to the Map
         The consumer.createMessageStreams is how we pass this information to Kafkaf
        */
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        //
        
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            ConsumerTest good = new ConsumerTest(stream,threadNumber);
            executor.submit(good);
            dispMessage += " " + good.displayMessage;
            
            threadNumber++;
        }
        
        return dispMessage;
    }
 
    //defining the basic configurations for the consumer to connect to kafka
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        // # of milliseconds Kafka will wait for ZooKeeper to respond to a request (read or write) before giving up and continuing to consume messages.
        props.put("zookeeper.session.timeout.ms", "4000");
        //number of milliseconds a ZooKeeper ‘follower’ can be behind the master before an error occurs
        props.put("zookeeper.sync.time.ms", "200");  
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
 
        return new ConsumerConfig(props);
    }
 
    public static void index() {
        String zooKeeper = "127.0.0.1:2181";
        String groupId = "test-consumer-group";
        String topic = "play";
        int threads = Integer.parseInt("2");
 
        Consumer example = new Consumer(zooKeeper, groupId, topic);
        String message = "Empt";
        message += " " + example.run(threads);
        
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
 
        }
        
        example.shutdown();
        render(message);
    }
}
