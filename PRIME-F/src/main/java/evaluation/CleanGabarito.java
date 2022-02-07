package evaluation;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.IdDuplicates;
import Parser.SerializationUtilities;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class CleanGabarito {

	public static void main(String[] args) {
		String INPUT_PATH1 = "inputs/dataset1_imdb";
		String INPUT_PATH2 = "inputs/dataset2_dbpedia";
		String INPUT_PATH_GROUNDTRUTH = "inputs/groundtruth_imdbdbpedia";
		
		
		List<EntityProfile> EntityListSource = null;
		List<EntityProfile> EntityListTarget = null;
		Set<IdDuplicates> newGT = new HashSet<IdDuplicates>();
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			
			Set<IdDuplicates> gt = (Set<IdDuplicates>) SerializationUtilities.loadSerializedObject(INPUT_PATH_GROUNDTRUTH);
			EntityListSource = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH1);
			EntityListTarget = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH2);
			
			
			for (IdDuplicates idDuplicates : gt) {
				EntityProfile entity1 = EntityListSource.get(idDuplicates.getEntityId1());
				EntityProfile entity2 = EntityListTarget.get(idDuplicates.getEntityId2());
				
				Set<Integer> tokens1 = getTokens(entity1);
				Set<Integer> tokens2 = getTokens(entity2);
				tokens1.retainAll(tokens2);
				
				if (!tokens1.isEmpty()) {
					newGT.add(idDuplicates);
				}
				
			}
			
			FileOutputStream fos = new FileOutputStream("inputs/groundtruth_imdbdbpedia_clean");
		    ObjectOutputStream oos = new ObjectOutputStream(fos);
		    
		    oos.writeObject(newGT);

		    oos.close();
			
			
//			//entity source
//			System.out.println("---- Entity 1 ----");
//			EntityListSource = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH1);
//			EntityProfile entity1 = EntityListSource.get(21261);//lembre de diminuir 1 do id (lista comeca por 0)
////			System.out.println(entity1.getEntityUrl());
//			for (Attribute att : entity1.getAttributes()) {
//				System.out.println(att.getName() + ": " + att.getValue());
//			}
//			System.out.println(getTokens(entity1).toString().replace(",", ""));
//			
//			System.out.println();
//			
//			//entity target
//			System.out.println("---- Entity 2 ----");
//			EntityListTarget = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH2);
//			EntityProfile entity2 = EntityListTarget.get(21810);//831,619,829,865,776,896,946,615,642//lembre de diminuir 1 do id (lista comeca por 0)
////			System.out.println(entity2.getEntityUrl());
//			for (Attribute att : entity2.getAttributes()) {
//				System.out.println(att.getName() + ": " + att.getValue());
//			}
//			System.out.println(getTokens(entity2).toString().replace(",", ""));
			
			ois1.close();
			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static Set<String> generateTokens(String string) {
		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
		Matcher m = p.matcher("");
		m.reset(string);
		String standardString = m.replaceAll("");
		
		KeywordGenerator kw = new KeywordGeneratorImpl();
		return kw.generateKeyWords(standardString);
	}
	
	private static Set<Integer> getTokens(EntityProfile se) {
		Set<Integer> cleanTokens = new HashSet<Integer>();

		for (Attribute att : se.getAttributes()) {
			KeywordGenerator kw = new KeywordGeneratorImpl();
			for (String string : generateTokens(att.getValue())) {
				cleanTokens.add(string.hashCode());
			}
		}
		
		return cleanTokens;
	}

}
