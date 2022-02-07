package DataProducer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

//localhost:9092 20 inputs/dataset1_abt true 0.1
public class KafkaDataStreamingProducerByTime2 {
	private static String INPUT_PATH1;
	private static boolean IS_SOURCE;
	private static double percentageOfEntitiesPerIncrement;
	private static int timer;
	private static Set<String> allTokens = new HashSet<String>();
	
	public static void main(String[] args) throws Exception {
		INPUT_PATH1 = args[2];
		IS_SOURCE = Boolean.parseBoolean(args[3]);
		percentageOfEntitiesPerIncrement = Double.parseDouble(args[4]); //number of entities per increment based on the percentage. e.g: 0,1 is 10%
		timer = Integer.parseInt(args[1]) * 1000;//second to milisecond
		
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		
		//TOPIC
        final String topicName = "mytopic";

        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<String>(
        		args[0],            // broker list
                topicName,                  // target topic
                new SimpleStringSchema());

		// add a simple source which is writing some strings
		DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

		// write stream to Kafka
		messageStream.addSink(producer);

		env.execute();
	}

	public static class SimpleStringGenerator implements SourceFunction<String> {
		private static final long serialVersionUID = 2174904787118597072L;
		boolean running = true;
		long i = 0;
		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			//CHOOSE THE INPUT PATHS
	        int currentIncrement = 0;
	        
//	        FlinkKafkaProducer011<String, String> producer = new FlinkKafkaProducer011<>(props);
	        ArrayList<EntityProfile> EntityListSource = null;
//	        ArrayList<EntityProfile> EntityListTarget = null;
	        
			// reading the files
			ObjectInputStream ois1;
			ObjectInputStream ois2;
			try {
				ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
//				ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
				EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
//				EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
				ois1.close();
//				ois2.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			int incrementControler = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
			int uniqueId = 0;
//			for (int i = 0; i < Math.max(EntityListSource.size(), EntityListTarget.size()); i++) {
			while (uniqueId < EntityListSource.size()) {
				currentIncrement++;
				ArrayList<String> listToSend = new ArrayList<String>();
				for (int i = uniqueId; i < incrementControler; i++) {
					if (i < EntityListSource.size()) {
						EntityProfile entitySource = EntityListSource.get(i);
						entitySource.setSource(IS_SOURCE);
						entitySource.setKey(uniqueId);
						entitySource.setIncrementID(currentIncrement);
						
						ctx.collect(entitySource.getStandardFormat());
						
						for (Attribute att : entitySource.getAttributes()) {
							KeywordGenerator kw = new KeywordGeneratorImpl();
							allTokens.addAll(kw.generateKeyWords(att.getValue()));
						}
						
					}
					
//					if (i < EntityListTarget.size()) {
//						EntityProfile entityTarget = EntityListTarget.get(i);
//						entityTarget.setSource(false);
//						entityTarget.setKey(uniqueId);
//						ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, entityTarget.getStandardFormat());
//			            producer.send(record2);
//					}
					
					uniqueId++;
				}
				
				
				incrementControler += (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
				System.out.println("Increment " + currentIncrement + " sent.");
				Thread.sleep(timer);
			}
			
	        System.out.println("----------------------------END------------------------------");
	        Calendar data = Calendar.getInstance();
	        int horas = data.get(Calendar.HOUR_OF_DAY);
	        int minutos = data.get(Calendar.MINUTE);
	        int segundos = data.get(Calendar.SECOND);
	        System.out.println(horas + ":" + minutos + ":" + segundos);
	        System.out.println("Number of possible tokens: " + allTokens.size());
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

}
