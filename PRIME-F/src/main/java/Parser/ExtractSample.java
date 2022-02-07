package Parser;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import DataStructures.EntityProfile;

public class ExtractSample {

	public static void main(String[] args) {
		String INPUT_PATH1 = args[0];
		int newSize = Integer.parseInt(args[1]);
//		String INPUT_PATH2 = "inputs/dataset2_dbpedia";
//		String INPUT_PATH_GROUNDTRUTH = "inputs/groundtruth_imdbdbpedia";
		
		
		List<EntityProfile> EntityListSource = null;
		List<EntityProfile> newListSource = new ArrayList<EntityProfile>();
//		Set<IdDuplicates> newGT = new HashSet<IdDuplicates>();
		// reading the files
		ObjectInputStream ois1;
		try {
			
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
//			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			
//			Set<IdDuplicates> gt = (Set<IdDuplicates>) SerializationUtilities.loadSerializedObject(INPUT_PATH_GROUNDTRUTH);
			EntityListSource = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH1);
//			EntityListTarget = (List<EntityProfile>) SerializationUtilities.loadSerializedObject(INPUT_PATH2);
			
			for (int i = 0; i < newSize; i++) {
				newListSource.add(EntityListSource.get(i));
			}
			
			FileOutputStream fos = new FileOutputStream(INPUT_PATH1 + "_sample"+newSize);
		    ObjectOutputStream oos = new ObjectOutputStream(fos);
		    
		    oos.writeObject(newListSource);

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
//			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

}
