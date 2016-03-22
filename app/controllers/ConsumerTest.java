/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package controllers;

/**
 *
 * @author root
 */
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import play.mvc.Controller;
 
public class ConsumerTest extends Controller implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    public String displayMessage ;
    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        displayMessage = "Emptymessage";
    }
 
    public void run() {
         // Consumred message to be displayed to the user
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()){
            displayMessage += "Thread " + m_threadNumber + ": " + new String(it.next().message());
            System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
            System.out.println("Shutting down Thread: " + m_threadNumber);
        }
        //render(displayMessage);
        
    }
}
