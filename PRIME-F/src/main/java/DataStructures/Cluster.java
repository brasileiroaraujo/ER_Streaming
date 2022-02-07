package DataStructures;

import java.util.HashSet;
import java.util.Set;

public class Cluster {
	private int tokenkey;
	private Set<Node> storedCollection;
	private Set<Node> newCollection;
	private int lastIncrementID;
	
	public Cluster(int tokenkey, Set<Node> storedCollection) {
		super();
		this.tokenkey = tokenkey;
		this.storedCollection = storedCollection;
	}
	
	public Cluster(int tokenkey, Set<Node> storedCollection, Set<Node> newCollection) {
		super();
		this.tokenkey = tokenkey;
		this.storedCollection = storedCollection;
		this.newCollection = newCollection;
	}
	
	public Cluster() {
	}

	public Cluster(Integer tokenkey, int incrementID, Set<Node> storedCollection, Set<Node> newCollection) {
		super();
		this.tokenkey = tokenkey;
		this.lastIncrementID = incrementID;
		this.storedCollection = storedCollection;
		this.newCollection = newCollection;
	}

	public int getTokenkey() {
		return tokenkey;
	}

	public void setTokenkey(int tokenkey) {
		this.tokenkey = tokenkey;
	}

	public Set<Node> getCollection() {
		return storedCollection;
	}

	public void setCollection(Set<Node> collection) {
		this.storedCollection = collection;
	}
	
	public void addAllCollection(Set<Node> collection) {
		this.storedCollection.addAll(collection);
	}
	
	public void addInCollection(Node value) {
		this.storedCollection.add(value);
	}
	
	
	public Set<Node> getNewCollection() {
		return newCollection;
	}

	public void setNewCollection(Set<Node> newCollection) {
		this.newCollection = newCollection;
	}
	
	public void addAllNewCollection(Set<Node> newCollection) {
		this.newCollection.addAll(newCollection);
	}
	
	public void addInNewCollection(Node value) {
		this.newCollection.add(value);
	}
	
	public int getLastIncrementID() {
		return lastIncrementID;
	}

	public void setLastIncrementID(int lastIncrementID) {
		this.lastIncrementID = lastIncrementID;
	}
	
	
	public void mergeCollections() {
		if (!newCollection.isEmpty()) {
//			markEntities();
			for (Node node : newCollection) {
				node.setMarked(true);
				storedCollection.add(node);
			}
			newCollection = new HashSet<Node>();
		}
	}
	
	public void markEntities() {
		for (Node node : storedCollection) {
			node.setMarked(false);
		}
		
	}
	

	@Override
	public String toString() {
		String out = "";
		for (Node node : getCollection()) {
			out += node.getId() + ", ";
		}
		return "" + getTokenkey() + ": " + out;
	}

	
}
