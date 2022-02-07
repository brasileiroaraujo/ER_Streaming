package DataStructures;

import java.util.HashSet;
import java.util.Set;

public class BlockStructureGraph {
	private Set<EntityNodeGraph> fromSource;
	private Set<EntityNodeGraph> fromTarget;
	private int id;
	private int currentIncrement;
	
	public BlockStructureGraph() {
		super();
		this.fromSource = new HashSet<EntityNodeGraph>();
		this.fromTarget = new HashSet<EntityNodeGraph>();
	}
	
	public void merge(BlockStructureGraph other) {
		fromSource.addAll(other.getFromSource());
		fromTarget.addAll(other.getFromTarget());
		currentIncrement = Math.max(currentIncrement, other.getCurrentIncrement());
	}
	
	public void addInSource(EntityNodeGraph node) {
		fromSource.add(node);
	}
	
	public void addInTarget(EntityNodeGraph node) {
		fromTarget.add(node);
	}

	public Set<EntityNodeGraph> getFromSource() {
		return fromSource;
	}

	public void setFromSource(Set<EntityNodeGraph> fromSource) {
		this.fromSource = fromSource;
	}

	public Set<EntityNodeGraph> getFromTarget() {
		return fromTarget;
	}

	public void setFromTarget(Set<EntityNodeGraph> fromTarget) {
		this.fromTarget = fromTarget;
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCurrentIncrement() {
		return currentIncrement;
	}

	public void setCurrentIncrement(int currentIncrement) {
		if (this.currentIncrement < currentIncrement) {
			this.currentIncrement = currentIncrement;
		}
	}

	public int size() {
		return fromSource.size() + fromTarget.size();
	}
	
}