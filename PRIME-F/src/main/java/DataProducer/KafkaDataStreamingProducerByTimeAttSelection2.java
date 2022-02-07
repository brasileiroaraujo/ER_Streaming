package DataProducer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.StatisticalSummarization;
import Parser.SerializationUtilities;
import info.debatty.java.lsh.MinHash;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

//localhost:9092 20 inputs/dataset1_abt true 0.1
public class KafkaDataStreamingProducerByTimeAttSelection2 {
	private static String INPUT_PATH1;
	private static String INPUT_PATH2;
	private static boolean APPLY_ATT_SELECTION;
	private static double percentageOfEntitiesPerIncrement;
	private static int timer;
	private static Set<String> allTokens = new HashSet<String>();
	
	public static void main(String[] args) throws Exception {
		INPUT_PATH1 = args[2];
        INPUT_PATH2 = args[3];
//		IS_SOURCE = Boolean.parseBoolean(args[4]);
		percentageOfEntitiesPerIncrement = Double.parseDouble(args[4]); //number of entities per increment based on the percentage. e.g: 0,1 is 10%
		timer = Integer.parseInt(args[1]) * 1000;//second to milisecond
		APPLY_ATT_SELECTION = Boolean.parseBoolean(args[5]);
		
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
	        List<EntityProfile> EntityListSource = null;
	        List<EntityProfile> EntityListTarget = null;
	        
			// reading the files
			ObjectInputStream ois1;
			ObjectInputStream ois2;
			try {
//				ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
//				ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
				EntityListSource = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH1);
				System.out.println("Source loaded ... " + EntityListSource.size());
				EntityListTarget = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH2);
				System.out.println("Target loaded ... " + EntityListTarget.size());
//				ois1.close();
//				ois2.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			if (APPLY_ATT_SELECTION) {
				System.out.println("Attribute selection starting ...");
				attributeSelection(EntityListSource, EntityListTarget);
				System.out.println("Attribute selection ending ...");
			}
			
			int incrementControlerSource = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
			int incrementControlerTarget = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListTarget.size());
			int uniqueIdSource = 0;
			int uniqueIdTarget = 0;
			
			
			while (uniqueIdSource < EntityListSource.size() || uniqueIdTarget < EntityListTarget.size()) {
				currentIncrement++;
				ArrayList<String> listToSend = new ArrayList<String>();
				for (int i = uniqueIdSource; i < incrementControlerSource; i++) {
					if (i < EntityListSource.size()) {
						EntityProfile entitySource = EntityListSource.get(i);
						entitySource.setSource(true); //is source
						entitySource.setKey(uniqueIdSource);
						entitySource.setIncrementID(currentIncrement);
						
						listToSend.add(entitySource.getStandardFormat());
						
						for (Attribute att : entitySource.getAttributes()) {
							KeywordGenerator kw = new KeywordGeneratorImpl();
							allTokens.addAll(kw.generateKeyWords(att.getValue()));
						}
						
					}
					
					uniqueIdSource++;
				}
				
				
				for (int i = uniqueIdTarget; i < incrementControlerTarget; i++) {
					if (i < EntityListTarget.size()) {
						EntityProfile entityTarget = EntityListTarget.get(i);
						entityTarget.setSource(false); //isn't source
						entityTarget.setKey(uniqueIdTarget);
						entityTarget.setIncrementID(currentIncrement);
						
						listToSend.add(entityTarget.getStandardFormat());
						
						for (Attribute att : entityTarget.getAttributes()) {
							KeywordGenerator kw = new KeywordGeneratorImpl();
							allTokens.addAll(kw.generateKeyWords(att.getValue()));
						}
						
					}
					
					uniqueIdTarget++;
				}
				
				for (String string : listToSend) {
					ctx.collect(string);
				}
				
				
				incrementControlerSource += (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
				incrementControlerTarget += (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListTarget.size());
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
		
		
		private void attributeSelection(List<EntityProfile> entityListSource,
				List<EntityProfile> entityListTarget) {
			HashMap<String, Integer> tokensIDs = new HashMap<String, Integer>();
	        HashMap<String, Set<Integer>> attributeMapSource = new HashMap<String, Set<Integer>>();
	        HashMap<String, Set<Integer>> attributeMapTarget = new HashMap<String, Set<Integer>>();
	        HashMap<Integer, Integer> itemsFrequencySource = new HashMap<Integer, Integer>();
	        HashMap<Integer, Integer> itemsFrequencyTarget = new HashMap<Integer, Integer>();
	        int currentTokenID = 0;
	        double totalTokensSource = 0;
	        double totalTokensTarget = 0;
	        
	        for (EntityProfile e : entityListSource) {
				for (Attribute att : e.getAttributes()) {
					KeywordGenerator kw = new KeywordGeneratorImpl();
					for (String tk : kw.generateKeyWords(att.getValue())) {
						if (!tokensIDs.containsKey(tk)) {
							tokensIDs.put(tk, currentTokenID++);
						}
						
						itemsFrequencySource.put(tokensIDs.get(tk), (itemsFrequencySource.getOrDefault(tokensIDs.get(tk), 0) + 1));
						
						if (attributeMapSource.containsKey(att.getName())) {
							attributeMapSource.get(att.getName()).add(tokensIDs.get(tk));
						} else {
							attributeMapSource.put(att.getName(), new HashSet<Integer>());
							attributeMapSource.get(att.getName()).add(tokensIDs.get(tk));
						}
						totalTokensSource++;
					}
				}
			}
	        
	        
	        for (EntityProfile e : entityListTarget) {
				for (Attribute att : e.getAttributes()) {
					KeywordGenerator kw = new KeywordGeneratorImpl();
					for (String tk : kw.generateKeyWords(att.getValue())) {
						if (!tokensIDs.containsKey(tk)) {
							tokensIDs.put(tk, currentTokenID++);
						}
						
						itemsFrequencyTarget.put(tokensIDs.get(tk), (itemsFrequencyTarget.getOrDefault(tokensIDs.get(tk), 0) + 1));
						
						if (attributeMapTarget.containsKey(att.getName())) {
							attributeMapTarget.get(att.getName()).add(tokensIDs.get(tk));
						} else {
							attributeMapTarget.put(att.getName(), new HashSet<Integer>());
							attributeMapTarget.get(att.getName()).add(tokensIDs.get(tk));
						}
						totalTokensTarget++;
					}
				}
			}
	        
	        
	        //blacklist based on entropy
			Set<Integer> blackListSource = StatisticalSummarization.getBlackListEntropy(attributeMapSource, itemsFrequencySource, totalTokensSource);
			Set<Integer> blackListTarget = StatisticalSummarization.getBlackListEntropy(attributeMapTarget, itemsFrequencyTarget, totalTokensTarget);
			
			//generate the MinHash
			HashMap<String, int[]> attributeHashesSource = new HashMap<String, int[]>();
			HashMap<String, int[]> attributeHashesTarget = new HashMap<String, int[]>();
			MinHash minhash = new MinHash(120, tokensIDs.size());//Estimate the signature size after
			for (String att : attributeMapSource.keySet()) {
				attributeHashesSource.put(att, minhash.signature(attributeMapSource.get(att)));
			}
			for (String att : attributeMapTarget.keySet()) {
				attributeHashesTarget.put(att, minhash.signature(attributeMapTarget.get(att)));
			}
			
			//generate the similarity matrix
			ArrayList<String> attFromSource = new ArrayList<String>(attributeHashesSource.keySet());
			ArrayList<String> attFromTarget = new ArrayList<String>(attributeHashesTarget.keySet());
			double[][] similarityMatrix = new double[attFromSource.size()][attFromTarget.size()];
			DescriptiveStatistics statistics = new DescriptiveStatistics();
			for (int i = 0; i < attFromSource.size(); i++) {
				for (int j = 0; j < attFromTarget.size(); j++) {
					double similarity = minhash.similarity(attributeHashesSource.get(attFromSource.get(i)), attributeHashesTarget.get(attFromTarget.get(j)));
					statistics.addValue(similarity);
					similarityMatrix[i][j] = similarity;
				}
			}
			
			//attribute selection (based on the matrix evaluation)
			double average = statistics.getMean();//similaritySum/(attFromSource.size() * attFromTarget.size());
//			double median = statistics.getPercentile(50); //the percentile in 50% represents the median
			
//			ArrayList<Integer> blackListSource = new ArrayList<Integer>();
//			ArrayList<Integer> blackListTarget = new ArrayList<Integer>();
			for (int i = 0; i < attFromSource.size(); i++) {
				int count = 0;
				for (int j = 0; j < attFromTarget.size(); j++) {
					if (similarityMatrix[i][j] >= average) { //use average or median
						count++;
					}
				}
				if (count == 0) {
					blackListSource.add(i);
				}
				count = 0;
			}			
			for (int i = 0; i < attFromTarget.size(); i++) {
				int count = 0;
				for (int j = 0; j < attFromSource.size(); j++) {
					if (similarityMatrix[j][i] >= average) {
						count++;
					}
				}
				if (count == 0) {
					blackListTarget.add(i);
				}
				count = 0;
			}
			
			//removing the bad attributes
			for (EntityProfile e : entityListSource) {
				for (Integer index : blackListSource) {
					Attribute att = findAttribute(e.getAttributes(), attFromSource.get(index));
					e.getAttributes().remove(att);
				}
			}
			for (EntityProfile e : entityListTarget) {
				for (Integer index : blackListTarget) {
					Attribute att = findAttribute(e.getAttributes(), attFromTarget.get(index));
					e.getAttributes().remove(att);
				}
			}
			
		}
		
		private static Attribute findAttribute(Set<Attribute> attributes, String string) {
			for (Attribute attribute : attributes) {
				if (attribute.getName().equalsIgnoreCase(string)) {
					return attribute;
				}
			}
			return null;
		}
	}

}
