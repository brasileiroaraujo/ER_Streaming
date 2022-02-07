package DataStructures;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ClusterGraph {
	private int tokenkey;
	private Set<NodeGraph> entitiesFromSource;
	private Set<NodeGraph> entitiesFromTarget;
//	private Set<String> comparisonsCount;
	private int currentIncrement;
	
	public ClusterGraph(int tokenkey, int increment) {
		super();
		this.tokenkey = tokenkey;
		this.entitiesFromSource = new HashSet<NodeGraph>();
		this.entitiesFromTarget = new HashSet<NodeGraph>();
		this.currentIncrement = increment;
//		this.comparisonsCount = new HashSet<String>();
	}
	
	public ClusterGraph(int tokenkey) {
		super();
		this.tokenkey = tokenkey;
		this.entitiesFromSource = new HashSet<NodeGraph>();
		this.entitiesFromTarget = new HashSet<NodeGraph>();
//		this.comparisonsCount = new HashSet<String>();
	}
	
//	public ClusterGraph(int tokenkey, Set<Node> storedCollection, Set<Node> newCollection) {
//		super();
//		this.tokenkey = tokenkey;
//		this.storedCollection = storedCollection;
//		this.newCollection = newCollection;
//	}
	
//	public ClusterGraph(Integer tokenkey, int incrementID, Set<Node> storedCollection, Set<Node> newCollection) {
//		super();
//		this.tokenkey = tokenkey;
//		this.lastIncrementID = incrementID;
//		this.storedCollection = storedCollection;
//		this.newCollection = newCollection;
//	}

	public int getTokenkey() {
		return tokenkey;
	}

	public void setTokenkey(int tokenkey) {
		this.tokenkey = tokenkey;
	}

	public Set<NodeGraph> getEntitiesFromSource() {
		return entitiesFromSource;
	}

	public void setEntitiesFromSource(Set<NodeGraph> entitiesFromSource) {
		this.entitiesFromSource = entitiesFromSource;
	}
	
	public void addInSource(NodeGraph node) {
		entitiesFromSource.add(node);
	}

	public Set<NodeGraph> getEntitiesFromTarget() {
		return entitiesFromTarget;
	}

	public void setEntitiesFromTarget(Set<NodeGraph> entitiesFromTarget) {
		this.entitiesFromTarget = entitiesFromTarget;
	}
	
	public void addInTarget(NodeGraph node) {
		entitiesFromTarget.add(node);
	}
	
	public int size() {
		return entitiesFromSource.size() + entitiesFromTarget.size();
	}

	@Override
	public String toString() {
		String out = "";
		for (NodeGraph node : getEntitiesFromSource()) {
			out += node.getId() + ", ";
		}
		return "" + getTokenkey() + ": " + out;
	}

	public void merge(ClusterGraph c2) {
		for (NodeGraph s : entitiesFromSource) {
			for (NodeGraph t : c2.getEntitiesFromTarget()) {
				double sim = calculateSimilarity(tokenkey, s.getBlocks(), t.getBlocks());
				if (sim > 0) {
					s.addNeighbor(new TupleSimilarity(t.getId(), sim));
				}
			}
		}
		
		for (NodeGraph s : c2.getEntitiesFromSource()) {
			for (NodeGraph t : entitiesFromTarget) {
				double sim = calculateSimilarity(tokenkey, s.getBlocks(), t.getBlocks());
				if (sim > 0) {
					s.addNeighbor(new TupleSimilarity(t.getId(), sim));
				}
			}
			entitiesFromSource.add(s);
		}
		
		entitiesFromTarget.addAll(c2.getEntitiesFromTarget());
		
	}
	
	public void merge2(ClusterGraph c2) {
//		for (NodeGraph s_c1 : entitiesFromSource) {
//			for (NodeGraph t_c2 : c2.getEntitiesFromTarget()) {
//				if (comparisonsCount.containsKey(s_c1.getId() + "-" + t_c2.getId())) {
//					comparisonsCount.replace(s_c1.getId() + "-" + t_c2.getId(), comparisonsCount.get(s_c1.getId() + "-" + t_c2.getId()) + 1);
//				} else {
//					comparisonsCount.put(s_c1.getId() + "-" + t_c2.getId(), 1);
//				}
//			}
//		}
//		
//		for (NodeGraph s_c2 : c2.getEntitiesFromSource()) {
//			for (NodeGraph t_c1 : entitiesFromTarget) {
//				if (comparisonsCount.containsKey(s_c2.getId() + "-" + t_c1.getId())) {
//					comparisonsCount.replace(s_c2.getId() + "-" + t_c1.getId(), comparisonsCount.get(s_c2.getId() + "-" + t_c1.getId()) + 1);
//				} else {
//					comparisonsCount.put(s_c2.getId() + "-" + t_c1.getId(), 1);
//				}
//			}
//		}
		
		currentIncrement = Math.max(currentIncrement, c2.getCurrentIncrement());
		entitiesFromSource.addAll(c2.getEntitiesFromSource());
		entitiesFromTarget.addAll(c2.getEntitiesFromTarget());
		
//		for (NodeGraph s : entitiesFromSource) {
//			for (NodeGraph t : entitiesFromTarget) {
//				comparisonsCount.add(s+"-"+t);
//			}
//		}
		
	}
	
//	public Set<String> getComparisonsCount() {
//		return comparisonsCount;
//	}
//
//	public void setComparisonsCount(Set<String> comparisonsCount) {
//		this.comparisonsCount = comparisonsCount;
//	}

	public int getCurrentIncrement() {
		return currentIncrement;
	}

	public void setCurrentIncrement(int currentIncrement) {
		this.currentIncrement = currentIncrement;
	}

	private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
		int minSize = Math.min(ent1.size() - 1, ent2.size() - 1);//<<<<< min 
		Set<Integer> intersect = new HashSet<Integer>(ent1);
		intersect.retainAll(ent2);

		// MACOBI strategy
		if (!Collections.min(intersect).equals(blockKey)) {
			return -1;
		}

		if (minSize > 0) {
			double x = (double) intersect.size() / minSize;
			return x;
		} else {
			return 0;
		}
	}


	
}
