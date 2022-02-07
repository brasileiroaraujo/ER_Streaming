package Parser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.StatisticalSummarization;
import info.debatty.java.lsh.MinHash;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class GenerateDatasetFormatted {
	private static String INPUT_PATH1;
	private static String INPUT_PATH2;
	private static String OUTPUT_PATH1;
	private static String OUTPUT_PATH2;
	private static boolean APPLY_ATT_SELECTION;
	private static double percentageOfEntitiesPerIncrement;
	private static int timer;
	private static Set<String> allTokens = new HashSet<String>();
	
	public static void main(String[] args) throws Exception {
		INPUT_PATH1 = args[0];
        INPUT_PATH2 = args[1];
//		IS_SOURCE = Boolean.parseBoolean(args[4]);
		APPLY_ATT_SELECTION = Boolean.parseBoolean(args[2]);
		OUTPUT_PATH1 = args[3];
		OUTPUT_PATH2 = args[4];
		
		
		FileWriter fw1 = new FileWriter(OUTPUT_PATH1, true);
		BufferedWriter bw1 = new BufferedWriter(fw1);
		
		FileWriter fw2 = new FileWriter(OUTPUT_PATH2, true);
		BufferedWriter bw2 = new BufferedWriter(fw2);
		
		
//		//CHOOSE THE INPUT PATHS
//        int currentIncrement = 0;
        
//        FlinkKafkaProducer011<String, String> producer = new FlinkKafkaProducer011<>(props);
        List<EntityProfile> EntityListSource = null;
        List<EntityProfile> EntityListTarget = null;
        
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
//			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
//			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			EntityListSource = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH1);
			System.out.println("Source loaded ... " + EntityListSource.size());
			EntityListTarget = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH2);
			System.out.println("Target loaded ... " + EntityListTarget.size());
//			ois1.close();
//			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		if (APPLY_ATT_SELECTION) {
			System.out.println("Attribute selection starting ...");
//			attributeSelection2(EntityListSource, EntityListTarget);
			HashMap<String, Integer> tokensIDs = new HashMap<String, Integer>();
	        HashMap<String, Set<Integer>> attributeMapSource = new HashMap<String, Set<Integer>>();
	        HashMap<String, Set<Integer>> attributeMapTarget = new HashMap<String, Set<Integer>>();
	        HashMap<Integer, Integer> itemsFrequencySource = new HashMap<Integer, Integer>();
	        HashMap<Integer, Integer> itemsFrequencyTarget = new HashMap<Integer, Integer>();
	        int currentTokenID = 0;
	        double totalTokensSource = 0;
	        double totalTokensTarget = 0;
	        
	        System.out.println("Extracting tokens from source ...");
			for (EntityProfile e : EntityListSource) {
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
			System.out.println("Extracted tokens from source ...");
			
			System.out.println("Extracting tokens from target ...");
			for (EntityProfile e : EntityListTarget) {
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
			System.out.println("Extracted tokens from target ...");
			
			
			//blacklist based on entropy
			System.out.println("Removing attributes from entropy ...");
			Set<Integer> blackListSource = StatisticalSummarization.getBlackListEntropy(attributeMapSource, itemsFrequencySource, totalTokensSource);
			System.out.println("Removed " + blackListSource.size() + "  attributes from source.");
			Set<Integer> blackListTarget = StatisticalSummarization.getBlackListEntropy(attributeMapTarget, itemsFrequencyTarget, totalTokensTarget);
			System.out.println("Removed " + blackListTarget.size() + "  attributes from target.");
			
			itemsFrequencySource = null;
			itemsFrequencyTarget = null;
			
			
			//generate the MinHash
			System.out.println("Generate minhash ...");
			HashMap<String, int[]> attributeHashesSource = new HashMap<String, int[]>();
			HashMap<String, int[]> attributeHashesTarget = new HashMap<String, int[]>();
			MinHash minhash = new MinHash(120, tokensIDs.size());//Estimate the signature size after
			tokensIDs=null;
			for (String att : attributeMapSource.keySet()) {
				attributeHashesSource.put(att, minhash.signature(attributeMapSource.get(att)));
			}
			for (String att : attributeMapTarget.keySet()) {
				attributeHashesTarget.put(att, minhash.signature(attributeMapTarget.get(att)));
			}
			System.out.println("Generated minhashs ...");
			
			attributeMapSource = null;
			attributeMapTarget = null;
			
			
			//generate the similarity matrix
			System.out.println("Creating similarity matrix ...");
			ArrayList<String> attFromSource = new ArrayList<String>(attributeHashesSource.keySet());
			ArrayList<String> attFromTarget = new ArrayList<String>(attributeHashesTarget.keySet());
			double[][] similarityMatrix = new double[attFromSource.size()][attFromTarget.size()];
			double sum = 0.0;
			for (int i = 0; i < attFromSource.size(); i++) {
				for (int j = 0; j < attFromTarget.size(); j++) {
					double similarity = minhash.similarity(attributeHashesSource.get(attFromSource.get(i)), attributeHashesTarget.get(attFromTarget.get(j)));
					sum += similarity;
					similarityMatrix[i][j] = similarity;
				}
			}
			System.out.println("Created similarity matrix ...");
			
			attributeHashesSource = null;
			attributeHashesTarget = null;
			
			double average = sum/(attFromSource.size()*attFromTarget.size());
			
			System.out.println("Defining the black list ...");
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
			System.out.println("Defined the black list ...");
			
			System.out.println("Removed in total " + blackListSource.size() + "  attributes from source.");
			System.out.println("Removed in total " + blackListTarget.size() + "  attributes from target.");
			
			//removing the bad attributes
			System.out.println("Removing the attributes for source ...");
			for (EntityProfile e : EntityListSource) {
				for (Integer index : blackListSource) {
					Attribute att = findAttribute(e.getAttributes(), attFromSource.get(index));
					e.getAttributes().remove(att);
				}
			}
			System.out.println("Removed the attributes for source ...");
			System.out.println("Removing the attributes for target ...");
			for (EntityProfile e : EntityListTarget) {
				for (Integer index : blackListTarget) {
					Attribute att = findAttribute(e.getAttributes(), attFromTarget.get(index));
					e.getAttributes().remove(att);
				}
			}
			System.out.println("Removed the attributes for target ...");
			System.out.println("Attribute selection ending ...");
			
			blackListSource=null;
			blackListTarget=null;
			similarityMatrix=null;
		}
		
//		int incrementControlerSource = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
//		int incrementControlerTarget = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListTarget.size());
		int uniqueIdSource = 0;
		int uniqueIdTarget = 0;
		
		
		System.out.println("Write DB1 starting ...");
		ArrayList<String> listToSend = new ArrayList<String>();
		while (uniqueIdSource < EntityListSource.size()) {
			EntityProfile entitySource = EntityListSource.get(uniqueIdSource);
			entitySource.setSource(true); //is source
			entitySource.setKey(uniqueIdSource);
			
			listToSend.add(entitySource.getStandardFormat2());
			
			for (Attribute att : entitySource.getAttributes()) {
				KeywordGenerator kw = new KeywordGeneratorImpl();
				allTokens.addAll(kw.generateKeyWords(att.getValue()));
			}
			
			uniqueIdSource++;
		}
		
		EntityListSource = null;
		for (String string : listToSend) {
			string = string.replaceAll("\n", "");
			bw1.write(string + "\n");
		}
		bw1.flush();
		bw1.close();
		fw1.close();
		System.out.println("Write DB1 ending ...");
				
		
		System.out.println("Write DB2 starting ...");
		listToSend = new ArrayList<String>();
		while (uniqueIdTarget < EntityListTarget.size()) {
			EntityProfile entityTarget = EntityListTarget.get(uniqueIdTarget);
			entityTarget.setSource(false); //isn't source
			entityTarget.setKey(uniqueIdTarget);
			
			listToSend.add(entityTarget.getStandardFormat2());
			
			for (Attribute att : entityTarget.getAttributes()) {
				KeywordGenerator kw = new KeywordGeneratorImpl();
				allTokens.addAll(kw.generateKeyWords(att.getValue()));
			}
			
			uniqueIdTarget++;

		}
		
		EntityListTarget = null;
		for (String string : listToSend) {
			string = string.replaceAll("\n", "");
			bw2.write(string + "\n");
		}
		bw2.flush();
		bw2.close();
		fw2.close();
		System.out.println("Write DB2 ending ...");
		
		
		
        System.out.println("----------------------------END------------------------------");
        Calendar data = Calendar.getInstance();
        int horas = data.get(Calendar.HOUR_OF_DAY);
        int minutos = data.get(Calendar.MINUTE);
        int segundos = data.get(Calendar.SECOND);
        System.out.println(horas + ":" + minutos + ":" + segundos);
        System.out.println("Number of possible tokens: " + allTokens.size());
		
		
	}

	private static void attributeSelection2(List<EntityProfile> entityListSource,
			List<EntityProfile> entityListTarget) {
		
//		HashMap<String, Integer> tokensIDs = new HashMap<String, Integer>();
//        HashMap<String, Set<Integer>> attributeMapSource = new HashMap<String, Set<Integer>>();
//        HashMap<String, Set<Integer>> attributeMapTarget = new HashMap<String, Set<Integer>>();
//        HashMap<Integer, Integer> itemsFrequencySource = new HashMap<Integer, Integer>();
//        HashMap<Integer, Integer> itemsFrequencyTarget = new HashMap<Integer, Integer>();
//        int currentTokenID = 0;
//        double totalTokensSource = 0;
//        double totalTokensTarget = 0;
//        
//        System.out.println("Extracting tokens from source ...");
//		for (EntityProfile e : entityListSource) {
//			for (Attribute att : e.getAttributes()) {
//				KeywordGenerator kw = new KeywordGeneratorImpl();
//				for (String tk : kw.generateKeyWords(att.getValue())) {
//					if (!tokensIDs.containsKey(tk)) {
//						tokensIDs.put(tk, currentTokenID++);
//					}
//					
//					itemsFrequencySource.put(tokensIDs.get(tk), (itemsFrequencySource.getOrDefault(tokensIDs.get(tk), 0) + 1));
//					
//					if (attributeMapSource.containsKey(att.getName())) {
//						attributeMapSource.get(att.getName()).add(tokensIDs.get(tk));
//					} else {
//						attributeMapSource.put(att.getName(), new HashSet<Integer>());
//						attributeMapSource.get(att.getName()).add(tokensIDs.get(tk));
//					}
//					totalTokensSource++;
//				}
//			}
//		}
//		System.out.println("Extracted tokens from source ...");
//		
//		System.out.println("Extracting tokens from target ...");
//		for (EntityProfile e : entityListTarget) {
//			for (Attribute att : e.getAttributes()) {
//				KeywordGenerator kw = new KeywordGeneratorImpl();
//				for (String tk : kw.generateKeyWords(att.getValue())) {
//					if (!tokensIDs.containsKey(tk)) {
//						tokensIDs.put(tk, currentTokenID++);
//					}
//					
//					itemsFrequencyTarget.put(tokensIDs.get(tk), (itemsFrequencyTarget.getOrDefault(tokensIDs.get(tk), 0) + 1));
//					
//					if (attributeMapTarget.containsKey(att.getName())) {
//						attributeMapTarget.get(att.getName()).add(tokensIDs.get(tk));
//					} else {
//						attributeMapTarget.put(att.getName(), new HashSet<Integer>());
//						attributeMapTarget.get(att.getName()).add(tokensIDs.get(tk));
//					}
//					totalTokensTarget++;
//				}
//			}
//		}
//		System.out.println("Extracted tokens from target ...");
//		
//		
//		//blacklist based on entropy
//		System.out.println("Removing attributes from entropy ...");
//		Set<Integer> blackListSource = StatisticalSummarization.getBlackListEntropy(attributeMapSource, itemsFrequencySource, totalTokensSource);
//		System.out.println("Removed " + blackListSource.size() + "  attributes from source.");
//		Set<Integer> blackListTarget = StatisticalSummarization.getBlackListEntropy(attributeMapTarget, itemsFrequencyTarget, totalTokensTarget);
//		System.out.println("Removed " + blackListTarget.size() + "  attributes from target.");
//		
//		itemsFrequencySource = null;
//		itemsFrequencyTarget = null;
//		
//		
//		//generate the MinHash
//		System.out.println("Generate minhash ...");
//		HashMap<String, int[]> attributeHashesSource = new HashMap<String, int[]>();
//		HashMap<String, int[]> attributeHashesTarget = new HashMap<String, int[]>();
//		MinHash minhash = new MinHash(120, tokensIDs.size());//Estimate the signature size after
//		for (String att : attributeMapSource.keySet()) {
//			attributeHashesSource.put(att, minhash.signature(attributeMapSource.get(att)));
//		}
//		for (String att : attributeMapTarget.keySet()) {
//			attributeHashesTarget.put(att, minhash.signature(attributeMapTarget.get(att)));
//		}
//		System.out.println("Generated minhashs ...");
//		
//		attributeMapSource = null;
//		attributeMapTarget = null;
//		
//		
//		//generate the similarity matrix
//		System.out.println("Creating similarity matrix ...");
//		ArrayList<String> attFromSource = new ArrayList<String>(attributeHashesSource.keySet());
//		ArrayList<String> attFromTarget = new ArrayList<String>(attributeHashesTarget.keySet());
//		double[][] similarityMatrix = new double[attFromSource.size()][attFromTarget.size()];
//		double sum = 0.0;
//		for (int i = 0; i < attFromSource.size(); i++) {
//			for (int j = 0; j < attFromTarget.size(); j++) {
//				double similarity = minhash.similarity(attributeHashesSource.get(attFromSource.get(i)), attributeHashesTarget.get(attFromTarget.get(j)));
//				sum += similarity;
//				similarityMatrix[i][j] = similarity;
//			}
//		}
//		System.out.println("Created similarity matrix ...");
//		
//		attributeHashesSource = null;
//		attributeHashesTarget = null;
//		
//		double average = sum/(attFromSource.size()*attFromTarget.size());
//		
//		System.out.println("Defining the black list ...");
//		for (int i = 0; i < attFromSource.size(); i++) {
//			int count = 0;
//			for (int j = 0; j < attFromTarget.size(); j++) {
//				if (similarityMatrix[i][j] >= average) { //use average or median
//					count++;
//				}
//			}
//			if (count == 0) {
//				blackListSource.add(i);
//			}
//			count = 0;
//		}			
//		for (int i = 0; i < attFromTarget.size(); i++) {
//			int count = 0;
//			for (int j = 0; j < attFromSource.size(); j++) {
//				if (similarityMatrix[j][i] >= average) {
//					count++;
//				}
//			}
//			if (count == 0) {
//				blackListTarget.add(i);
//			}
//			count = 0;
//		}
//		System.out.println("Defined the black list ...");
//		
//		//removing the bad attributes
//		System.out.println("Removing the attributes for source ...");
//		for (EntityProfile e : entityListSource) {
//			for (Integer index : blackListSource) {
//				Attribute att = findAttribute(e.getAttributes(), attFromSource.get(index));
//				e.getAttributes().remove(att);
//			}
//		}
//		System.out.println("Removed the attributes for source ...");
//		System.out.println("Removing the attributes for target ...");
//		for (EntityProfile e : entityListTarget) {
//			for (Integer index : blackListTarget) {
//				Attribute att = findAttribute(e.getAttributes(), attFromTarget.get(index));
//				e.getAttributes().remove(att);
//			}
//		}
//		System.out.println("Removed the attributes for target ...");
		
		
	}

	private static void attributeSelection(List<EntityProfile> entityListSource,
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
//		DescriptiveStatistics statistics = new DescriptiveStatistics();
		double sum = 0.0;
		for (int i = 0; i < attFromSource.size(); i++) {
			for (int j = 0; j < attFromTarget.size(); j++) {
				double similarity = minhash.similarity(attributeHashesSource.get(attFromSource.get(i)), attributeHashesTarget.get(attFromTarget.get(j)));
				sum += similarity;
//				statistics.addValue(similarity);
				similarityMatrix[i][j] = similarity;
			}
		}
		
		//attribute selection (based on the matrix evaluation)
//		double average = statistics.getMean();//similaritySum/(attFromSource.size() * attFromTarget.size());
		double average = sum/(attFromSource.size()*attFromTarget.size());
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
