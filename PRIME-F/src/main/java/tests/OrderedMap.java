package tests;

import java.util.TreeSet;

import DataStructures.TupleSimilarity;

public class OrderedMap {

	public static void main(String[] args) {
//		TreeMap<Integer, Double> map = new TreeMap<Integer, Double>();
//		map.put(1, 0.9);
//		map.put(3, 0.3);
//		map.put(2, 0.5);
//		map.put(1, 0.1);
//		
//		System.out.println(map);
		
		TreeSet<TupleSimilarity> neighbors = new TreeSet<TupleSimilarity>();
		TupleSimilarity a = new TupleSimilarity(1, 0.8);
		neighbors.add(a);
		
		TupleSimilarity b = new TupleSimilarity(1, 0.7);
		
		if (neighbors.contains(b)) {
			neighbors.remove(b);
			neighbors.add(b);
		}
		
		System.out.println(neighbors);

	}

}
