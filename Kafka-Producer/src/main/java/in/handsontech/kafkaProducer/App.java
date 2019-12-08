package in.handsontech.kafkaProducer;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author paripate
 * Project created using Maven
 * Change the bellow configuration with yours
 */
public class App 
{
    public static void main( String[] args )
    {
    	try {
    		//Name of the topic that you have created with kafka that you want to use with this project
	    	String topicName = "testtopic";
	        Properties props = new Properties();
	        props.put("bootstrap.servers", "localhost:9092");   //Enter the address where the kafka is hosted with the port number
	        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        //Set acknowledgements for producer requests.      
	        props.put("acks", "all");
	        
	        //If the request fails, the producer can automatically retry,
	        props.put("retries", 0);
	        
	        //Specify buffer size in config
	        props.put("batch.size", 16384);
	        
	        //Reduce the no of requests less than 0   
	        props.put("linger.ms", 1);
	        
	        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
	        props.put("buffer.memory", 33554432);
	
	        
	        Producer<String, String> producer = new KafkaProducer<String, String>(props);
	        //Code to write to the kafka using KafkaProducer, to quit the program you can use ':q'
	        while(true) {
	        	Scanner scnr = new Scanner(System.in);
	        	String val = scnr.nextLine();
	        	//write :q in the console to quit the program
	        	if(val.startsWith(":q")) {
	        		break;
	        	}
	        	producer.send(new ProducerRecord<String, String>(topicName, " > " + val));
	        }
	        System.out.println("SupplierProducer Completed.");
	        producer.close();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
}
