package DataStructures;

import java.util.HashSet;
import java.util.Set;

public class MetablockinNodeAggregation {
	private Set<MetablockingNode> source;
	private Set<MetablockingNode> target;
	private Integer blockId;
//	private Boolean isSource;
	
	public MetablockinNodeAggregation() {
		super();
		this.source = new HashSet<MetablockingNode>();
		this.target = new HashSet<MetablockingNode>();
	}
	
	public void merge(MetablockinNodeAggregation other) {
		if (other.getBlockId() != null) {
			setBlockId(other.getBlockId());
		}
		source.addAll(other.getSource());
		target.addAll(other.getTarget());
	}


//	public Boolean isSource() {
//		return isSource;
//	}
//
//	public void setSource(boolean isSource) {
//		this.isSource = isSource;
//	}
	
	public void addBlock(MetablockingNode node) {
		if (node.isSource()) {
			source.add(node);
		} else {
			target.add(node);
		}
	}


	public Integer getBlockId() {
		return blockId;
	}

	public void setBlockId(Integer blockId) {
		this.blockId = blockId;
	}

	public Set<MetablockingNode> getSource() {
		return source;
	}

	public void setSource(Set<MetablockingNode> source) {
		this.source = source;
	}

	public Set<MetablockingNode> getTarget() {
		return target;
	}

	public void setTarget(Set<MetablockingNode> target) {
		this.target = target;
	}
	
	
	
}
