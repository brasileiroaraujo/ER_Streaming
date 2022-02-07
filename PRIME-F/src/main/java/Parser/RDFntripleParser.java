package Parser;

import java.util.List;

import DataStructures.EntityProfile;

public class RDFntripleParser {
	
//	public static void main(String[] args) {
//		String fileNameOrUri = "inputs/locah.nt";
//	    Model model = ModelFactory.createDefaultModel();
//	    InputStream is = FileManager.get().open(fileNameOrUri);
//	    if (is != null) {
//	        Graph g = model.read(is, null, "N-TRIPLES").getGraph();
//	        ExtendedIterator<Triple> x = g.find();
//	        while (x.hasNext()) {
//				System.out.println(x.next());
//			}
////	        System.out.println();
////	        model.write(System.out, "TURTLE");
//	    } else {
//	        System.err.println("cannot read " + fileNameOrUri);;
//	    }
//	}
	
//	public static void main(String[] args) throws FileNotFoundException {
//		FileInputStream is = new FileInputStream("inputs/locah.nt");
//
//		NxParser nxp = new NxParser();
//		nxp.parse(is);
//
//		for (Node[] nx : nxp)
//		  System.out.println(nx[0].getLabel());
//	}
//	public static void main(String[] args) {
//	    String fileNameOrUri = "inputs/locah.nt";
//	    Model model = ModelFactory.createDefaultModel();
//	    InputStream is = FileManager.get().open(fileNameOrUri);
//	    if (is != null) {
//	        model.read(is, null, "N-TRIPLE");
//	        model.write(System.out, "TURTLE");
//	    } else {
//	        System.err.println("cannot read " + fileNameOrUri);;
//	    }
//	}
	
	public static void main(String[] args) {
		@SuppressWarnings("unchecked")
		List<EntityProfile> x = (List<EntityProfile>) SerializationUtilities.loadSerializedObject("inputs/dataset1_big_dbpedia");
		System.out.println(x.size());
		
	}
	
	
}
