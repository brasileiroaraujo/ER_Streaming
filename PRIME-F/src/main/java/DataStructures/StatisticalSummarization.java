package DataStructures;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Set;

public class StatisticalSummarization {
	
	public static double getEntropy(boolean normalized, Set<Integer> itemsFrequency, HashMap<Integer, Integer> itemsFrequencySource, double totalTokens) {
        double entropy = 0.0;
        double len = totalTokens;
        for (Integer entry : itemsFrequency) {
            double p_i = (itemsFrequencySource.get(entry) / len);//considering the existence of one token in this attribute (using a set structure).
            entropy -= (p_i * (Math.log10(p_i) / Math.log10(2.0d)));
        }
        if (normalized) {
            return entropy / getMaxEntropy(len);
        } else { //commonly this one is used.
            return entropy;
        }
    }
	
	public static double getEntropy2(boolean normalized, Set<Integer> itemsFrequency, HashMap<Integer, Integer> itemsFrequencySource, double totalTokens) {
		double entropy = 0.0;
        for (Integer entry : itemsFrequency) {
            double p = 1.0 * itemsFrequencySource.get(entry) / totalTokens;
            if (itemsFrequencySource.get(entry) > 0)  
                entropy -= p * Math.log(p) / Math.log(2);
        }
        return entropy;
    }
	
//	public double getEntropyToken(boolean normalized, Map<String, Integer>itemsFrequency, int noOfTotalTerms) {
//        double entropy = 0.0;
//        double len = noOfTotalTerms;
//        for (Entry<String, Integer> entry : itemsFrequency.entrySet()) {
//            double p_i = (entry.getValue() / len);
//            entropy -= (p_i * (Math.log10(p_i) / Math.log10(2.0d)));
//        }
//        if (normalized) {
//            return entropy / getMaxEntropy(len);
//        } else { //commonly this one is used.
//            return entropy;
//        }
//    }
	
	private static double getMaxEntropy(double N) {
        double entropy = Math.log10(N) / Math.log10(2.0d);
        return entropy;
    }

	public static Set<Integer> getBlackListEntropy(HashMap<String, Set<Integer>> attributeMap, HashMap<Integer, Integer> itemsFrequency, double totalTokens) {
		DescriptiveStatistics statistics = new DescriptiveStatistics();
		HashMap<Integer, Double> entropies = new HashMap<Integer, Double>();
		Set<Integer> blackList = new HashSet<Integer>();
		int index = 0;
		
		for (Entry<String, Set<Integer>> tuple : attributeMap.entrySet()) {
			double entropyValue = getEntropy(false, tuple.getValue(), itemsFrequency, totalTokens);
			statistics.addValue(entropyValue);
			entropies.put(index++, entropyValue);
		}
		
		double threshold = statistics.getPercentile(40);//think better about low entropies.
		
		for (Entry<Integer, Double> tuple : entropies.entrySet()) {
			if (tuple.getValue() < threshold) {
				blackList.add(tuple.getKey());
			}
		}
		return blackList;
	}

}
