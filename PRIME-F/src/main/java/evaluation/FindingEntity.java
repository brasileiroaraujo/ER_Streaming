package evaluation;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import Parser.SerializationUtilities;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class FindingEntity {

	public static void main(String[] args) {
		String INPUT_PATH1 = "inputs/dataset1_big_dbpedia_sample50000";
		String INPUT_PATH2 = "inputs/dataset2_big_dbpedia_sample100000";
		
		
		List<EntityProfile> EntityListSource = null;
		List<EntityProfile> EntityListTarget = null;
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			
			//entity source
			System.out.println("---- Entity 1 ----");
			EntityListSource = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH1);
			EntityProfile entity1 = EntityListSource.get(30327);//lembre de diminuir 1 do id (lista comeca por 0)
//			System.out.println(entity1.getEntityUrl());
			for (Attribute att : entity1.getAttributes()) {
				System.out.println(att.getName() + ": " + att.getValue());
			}
			System.out.println(getTokens(entity1).toString().replace(",", ""));
			
			System.out.println();
			
			//entity target
			System.out.println("---- Entity 2 ----");
			EntityListTarget = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH2);
			EntityProfile entity2 = EntityListTarget.get(56052);//831,619,829,865,776,896,946,615,642//lembre de diminuir 1 do id (lista comeca por 0)
//			System.out.println(entity2.getEntityUrl());
			for (Attribute att : entity2.getAttributes()) {
				System.out.println(att.getName() + ": " + att.getValue());
			}
			System.out.println(getTokens(entity2).toString().replace(",", ""));
			
			ois1.close();
			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
//	private static Set<String> generateTokens(String string) {
//		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
//		Matcher m = p.matcher("");
//		m.reset(string);
//		String standardString = m.replaceAll("");
//		
//		KeywordGenerator kw = new KeywordGeneratorImpl();
//		return kw.generateKeyWords(standardString);
//	}
	
	private static Set<Integer> generateTokens(String string) {
		if (string.length() > 20 && string.substring(0, 20).equals("<http://dbpedia.org/")) {
			String[] uriPath = string.split("/");
			string = Arrays.toString(uriPath[uriPath.length-1].split("[\\W_]")).toLowerCase();
		}
		
		//To improve quality, use the following code
		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
		Matcher m = p.matcher("");
		m.reset(string);
		String standardString = m.replaceAll("");
		
		KeywordGenerator kw = new KeywordGeneratorImpl();
		return kw.generateKeyWordsHashCode(standardString);
	}
	
	private static Set<Integer> getTokens(EntityProfile se) {
		Set<Integer> cleanTokens = new HashSet<Integer>();

		for (Attribute att : se.getAttributes()) {
			for (Integer string : generateTokens(att.getValue())) {
				cleanTokens.add(string);
			}
		}
		
		return cleanTokens;
	}

}
