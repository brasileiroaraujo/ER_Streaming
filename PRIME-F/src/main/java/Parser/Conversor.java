package Parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import DataStructures.EntityProfile;

public class Conversor {

	public static void main(String[] args) {
		// reading the files
		ObjectInputStream ois1;
		ArrayList<EntityProfile> EntityListSource = null;
		

		try {
			FileOutputStream f = new FileOutputStream(new File("inputs/dataset1_big_dbpedia_cutted"));
			ObjectOutputStream o = new ObjectOutputStream(f);
			
			ois1 = new ObjectInputStream(new FileInputStream("inputs/dataset1_big_dbpedia"));
//			EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
			
			o.writeObject(ois1.readObject());
			
			o.close();
			f.close();
			ois1.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Size source: " + EntityListSource.size());

	}

}
