package DataStructures;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.math3.stat.descriptive.moment.Variance;

import NewsAPI.R7API;
import scala.Tuple2;

public class EntityNode {
	private String token;
	private Object id;
	private Set<String> blocks;
	private TreeSet<TupleSimilarity2> neighbors;
	private boolean isSource;
	private int maxNumberOfNeighbors;
	private Integer incrementId;
	
	private String text;
	
	
	public EntityNode(String token, Object id, Set<String> blocks, boolean isSource, int maxNumberOfNeighbors, Integer incrementId) {
		super();
		this.token = token;
		this.id = id;
		this.blocks = blocks;
		this.neighbors = new TreeSet<TupleSimilarity2>();
		this.isSource = isSource;
		this.maxNumberOfNeighbors = maxNumberOfNeighbors;
		this.incrementId = incrementId;
	}
	
	public EntityNode() {
		super();
		this.id = -100L;
		this.blocks = new HashSet<>();
		this.neighbors = new TreeSet<>();
		this.isSource = false;
	}
	
	public boolean isSource() {
		return isSource;
	}


	public void setSource(boolean isSource) {
		this.isSource = isSource;
	}


	public String getIdAsString() {
		return (String) id;
	}
	
	public Long getIdAsLong() {
		return (Long) id;
	}
	
	public Integer getIdAsInteger() {
		return (Integer) id;
	}


	public void setId(Long id) {
		this.id = id;
	}
	
	public void setId(String id) {
		this.id = id;
	}

	public Set<String> getBlocks() {
		return blocks;
	}


	public void setBlocks(Set<String> blocks) {
		this.blocks = blocks;
	}


	public Set<TupleSimilarity2> getNeighbors() {
		return neighbors;
	}
	

//	public void setNeighbors(Set<Tuple2<Integer, Double>> neighbors) {
//		this.neighbors = neighbors;
//	}
	
//	public void addNeighbor(Tuple2<Integer, Double> neighbor) {
//		this.sumWeight += neighbor._2();
//		this.numberOfNeighbors ++;
//		this.neighbors.add(neighbor);
//	}
//	
	public void addAllNeighbors(Set<TupleSimilarity2> neighborsInput) {
		this.neighbors.addAll(neighborsInput);
		while (neighbors.size() > maxNumberOfNeighbors) {
			neighbors.pollLast();
		}
	}
	
	public void addNeighbor(TupleSimilarity2 tupleSimilarity2) {
//		if (this.neighbors.contains(neighbor)) {
//			this.neighbors.remove(neighbor);
//			this.neighbors.add(neighbor);
//		} else {
//			this.neighbors.add(neighbor);
//		}
		if (!this.neighbors.contains(tupleSimilarity2)) {
			this.neighbors.add(tupleSimilarity2);
		}
		
		if (neighbors.size() > maxNumberOfNeighbors) {
			neighbors.pollLast();
		}
	}
	
//	public void addAllNeighbor(NodeGraph n2) {
//		this.neighbors.addAll(n2.getNeighbors());
//	}
	
//	public void addAllNeighbor(Set<Tuple2<Integer, Double>> neighbors) {
//		for (Tuple2<Integer, Double> neighbor : neighbors) {
//			addNeighbor(neighbor);
//		}
//	}
	
	public void pruningWNP() {
		double threshold = getMeanDistribution();
		TreeSet<TupleSimilarity2> prunnedNeighbors = new TreeSet<>();
		
		for (TupleSimilarity2 neighbor : neighbors) {
			if (neighbor.getValue() >= threshold) {
				prunnedNeighbors.add(neighbor);
			}
		}
		
		this.neighbors = prunnedNeighbors;
		
	}
	
	public void pruningOutliers() {
		Tuple2<Double, Double> meanAndDP = getMeanAndDPDistribution();
		double threshold = meanAndDP._1() - meanAndDP._2();
		TreeSet<TupleSimilarity2> prunnedNeighbors = new TreeSet<>();
		
		for (TupleSimilarity2 neighbor : neighbors) {
			if (neighbor.getValue() >= threshold) {
				prunnedNeighbors.add(neighbor);
			}
		}
		
		this.neighbors = prunnedNeighbors;
		
	}
	
	private Tuple2<Double, Double> getMeanAndDPDistribution() {
		double numberOfNeighbors = neighbors.size();
		double sumWeight = 0.0;
		double maxSim = -1;
		double[] values = new double[neighbors.size()];
		int index = 0;
		
		for (TupleSimilarity2 neighbor : neighbors) {
			sumWeight += neighbor.getValue();
			values[index] = neighbor.getValue();
			if (neighbor.getValue() > maxSim) {
				maxSim = neighbor.getValue();
			}
		}
		
		return new Tuple2<Double, Double>((sumWeight/numberOfNeighbors), Math.sqrt(new Variance().evaluate(values)));
	}

	private double getMeanDistribution() {
		double numberOfNeighbors = neighbors.size();
		double sumWeight = 0.0;
		
		for (TupleSimilarity2 neighbor : neighbors) {
			sumWeight += neighbor.getValue();
		}
		return sumWeight/numberOfNeighbors;
	}
	
	private Tuple2<Double, Double> getMeanAndMaxDistribution() {
		double numberOfNeighbors = neighbors.size();
		double sumWeight = 0.0;
		double maxSim = -1;
		
		for (TupleSimilarity2 neighbor : neighbors) {
			sumWeight += neighbor.getValue();
			if (neighbor.getValue() > maxSim) {
				maxSim = neighbor.getValue();
			}
		}
		return new Tuple2<Double, Double>((sumWeight/numberOfNeighbors), maxSim);
	}

	public String getToken() {
		return token;
	}


	public void setToken(String newToken) {
		this.token = newToken;
	}


	public void setMaxNumberOfNeighbors(int maxNumberOfNeighbors) {
		this.maxNumberOfNeighbors = maxNumberOfNeighbors;
	}

	public int getIncrementId() {
		return incrementId;
	}

	public void setIncrementId(int incrementId) {
		this.incrementId = incrementId;
	}
	
	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	@Override
	public String toString() {
//		String output = id + ":";
		String output = "";
		for (TupleSimilarity2 neighbor : neighbors) {
			output += neighbor.getKey() + ",";
		}
		if (!neighbors.isEmpty()) {
			return output.substring(0,output.length()-1);
		} else {
			return output;
		}
		
	}
	
//	@Override
//	public String toString() {
////		String output = id + ":";
//		String output = "";
//		for (TupleSimilarity2 neighbor : neighbors) {
//			output += neighbor.getText() + "\n\n";
//		}
//		return output;
////		if (!neighbors.isEmpty()) {
////			return output.substring(0,output.length()-1);
////		} else {
////			return output;
////		}
//		
//	}
	
	@Override
	public int hashCode() {
	    return id.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (id instanceof Long) {
			return id.equals(((EntityNode)obj).getIdAsLong());
		}
		return id.equals(((EntityNode)obj).getIdAsString());
	}

}
