package DataStructures;

import java.util.HashSet;
import java.util.Set;

public class BlockStructure {
	private Set<EntityNode> fromSource;
	private Set<EntityNode> fromTarget;
	private int id;
	private int currentIncrement;
	
	public BlockStructure() {
		super();
		this.fromSource = new HashSet<EntityNode>();
		this.fromTarget = new HashSet<EntityNode>();
	}
	
	public void merge(BlockStructure other) {
		fromSource.addAll(other.getFromSource());
		fromTarget.addAll(other.getFromTarget());
		currentIncrement = Math.max(currentIncrement, other.getCurrentIncrement());
	}
	
	public void addInSource(EntityNode node) {
		fromSource.add(node);
	}
	
	public void addInTarget(EntityNode node) {
		fromTarget.add(node);
	}

	public Set<EntityNode> getFromSource() {
		return fromSource;
	}

	public void setFromSource(Set<EntityNode> fromSource) {
		this.fromSource = fromSource;
	}

	public Set<EntityNode> getFromTarget() {
		return fromTarget;
	}

	public void setFromTarget(Set<EntityNode> fromTarget) {
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
