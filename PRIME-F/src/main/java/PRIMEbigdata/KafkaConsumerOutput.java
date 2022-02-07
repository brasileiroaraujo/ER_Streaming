package PRIMEbigdata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

//https://dzone.com/articles/writing-a-kafka-consumer-in-java
public class KafkaConsumerOutput {
	
	private final static String TOPIC = "output";
    private static String BOOTSTRAP_SERVERS = "";
    private static String OUTPUT_PATH1 = "";
    private static int GIVE_UP = 0;
    private static int POOL_TIME = 0;
    
    //"localhost:9092,localhost:9093,localhost:9094";
	
    public static void main(String[] args) throws Exception {
    	BOOTSTRAP_SERVERS = args[0];
    	OUTPUT_PATH1 = args[1];
    	GIVE_UP = Integer.parseInt(args[2]);
    	POOL_TIME = Integer.parseInt(args[3]);
        runConsumer();
    }
	
	private static Consumer<Long, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// Create the consumer using props.
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}
	
	static void runConsumer() throws InterruptedException, IOException {
		FileWriter fw1 = new FileWriter(OUTPUT_PATH1, true);
		BufferedWriter bw1 = new BufferedWriter(fw1);
		
		final Consumer<Long, String> consumer = createConsumer();
		
		final int giveUp = GIVE_UP;
		int noRecordsCount = 0;
		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(POOL_TIME * 1000);//time in miliseconds
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			} else {
				noRecordsCount = 0;
//				bw1.flush();
			}
			
			
			
			consumerRecords.forEach(record -> {
				try {
					bw1.write(record.value() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
//				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
//						record.partition(), record.offset());
			});
			bw1.flush();
			consumer.commitAsync();
		}
		consumer.close();
		System.out.println("DONE");
		
		bw1.flush();
		bw1.close();
		fw1.close();
	}

}
