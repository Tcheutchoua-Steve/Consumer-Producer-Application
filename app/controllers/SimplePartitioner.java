/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package controllers;

/**
 *
 * @author Tcheutchoua Steve
 */
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import play.mvc.Controller;
 
public class SimplePartitioner extends Controller implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
 
    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
        }
       return partition;
  }
 
}
