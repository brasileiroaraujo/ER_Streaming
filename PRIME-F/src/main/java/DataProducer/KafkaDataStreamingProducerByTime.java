package DataProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import DataStructures.EntityProfile;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

//localhost:9092 20 inputs/dataset1_abt true 0.1
public class KafkaDataStreamingProducerByTime {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);//localhost:9092    10.171.171.50:8088
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
//        int[] timers = {10, 25, 50, 100, 250};
        int timer = Integer.parseInt(args[1]) * 1000;//second to milisecond
        Random random = new Random();
        
        //TOPIC
        final String topicName = "mytopic";
        
        //CHOOSE THE INPUT PATHS
        String INPUT_PATH1 = args[2];
        boolean IS_SOURCE = Boolean.parseBoolean(args[3]);
        double percentageOfEntitiesPerIncrement = Double.parseDouble(args[4]); //number of entities per increment based on the percentage. e.g: 0,1 is 10% 
        int currentIncrement = 0;
//        String INPUT_PATH2 = args[3];
        
//        String INPUT_PATH1 = "inputs/dataset1_imdb";
//        String INPUT_PATH2 = "inputs/dataset2_dbpedia";
        
//        String INPUT_PATH1 = "inputs/dataset1_dblp2";
//        String INPUT_PATH2 = "inputs/dataset2_scholar";
        
//        String INPUT_PATH1 = "inputs/dataset1_dblp";
//        String INPUT_PATH2 = "inputs/dataset2_acm";
        
//        String INPUT_PATH1 = "inputs/dataset1_amazon";
//        String INPUT_PATH2 = "inputs/dataset2_gp";
        
//        String INPUT_PATH1 = "inputs/dataset1_abt";
//        String INPUT_PATH2 = "inputs/dataset2_buy";

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ArrayList<EntityProfile> EntityListSource = null;
//        ArrayList<EntityProfile> EntityListTarget = null;
        
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
//			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
//			EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
			ois1.close();
//			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int incrementControler = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
		int uniqueId = 0;
//		for (int i = 0; i < Math.max(EntityListSource.size(), EntityListTarget.size()); i++) {
		while (uniqueId < EntityListSource.size()) {
			currentIncrement++;
			for (int i = uniqueId; i < incrementControler; i++) {
				if (i < EntityListSource.size()) {
					EntityProfile entitySource = EntityListSource.get(i);
					entitySource.setSource(IS_SOURCE);
					entitySource.setKey(uniqueId);
					entitySource.setIncrementID(currentIncrement);
					ProducerRecord<String, String> record = new ProducerRecord<>(topicName, entitySource.getStandardFormat());
		            producer.send(record);
				}
				
//				if (i < EntityListTarget.size()) {
//					EntityProfile entityTarget = EntityListTarget.get(i);
//					entityTarget.setSource(false);
//					entityTarget.setKey(uniqueId);
//					ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, entityTarget.getStandardFormat());
//		            producer.send(record2);
//				}
				
				uniqueId++;
			}
			incrementControler += (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
			System.out.println("Increment " + currentIncrement + " sent.");
			Thread.sleep(timer);
		}
		
		
//		ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "THEEND");
//        producer.send(record);
		
        producer.close();
        System.out.println("----------------------------END------------------------------");
        Calendar data = Calendar.getInstance();
        int horas = data.get(Calendar.HOUR_OF_DAY);
        int minutos = data.get(Calendar.MINUTE);
        int segundos = data.get(Calendar.SECOND);
        System.out.println(horas + ":" + minutos + ":" + segundos);
        
//        throw new Exception();
    }
}
