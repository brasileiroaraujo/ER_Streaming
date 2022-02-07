package Parser;

import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.Range;

import DataStructures.IdDuplicates;

public class ExtractSampleGroundtruth {

	public static void main(String[] args) {
		String INPUT_PATH_GROUNDTRUTH = args[0];
		Range<Integer> range1 = Range.between(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
		Range<Integer> range2 = Range.between(Integer.parseInt(args[3]), Integer.parseInt(args[4]));
//		String INPUT_PATH2 = "inputs/dataset2_dbpedia";
		
		
		Set<IdDuplicates> newGT = new HashSet<IdDuplicates>();
		// reading the files
		ObjectInputStream ois1;
		try {
			
//			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
//			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			
			Set<IdDuplicates> gt = (Set<IdDuplicates>) SerializationUtilities.loadSerializedObject(INPUT_PATH_GROUNDTRUTH);
//			EntityListSource = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH_GROUNDTRUTH);
//			EntityListTarget = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH2);
			
			for (IdDuplicates idDuplicates : gt) {
				if (range1.contains(idDuplicates.getEntityId1()) && range2.contains(idDuplicates.getEntityId2())) {
					newGT.add(idDuplicates);
				}
			}
			
			System.out.println("New number of duplicates: " + newGT.size());
			
			FileOutputStream fos = new FileOutputStream(INPUT_PATH_GROUNDTRUTH + "_sample_"+args[2]+"_"+args[4]);
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
			
//			ois1.close();
//			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

}
