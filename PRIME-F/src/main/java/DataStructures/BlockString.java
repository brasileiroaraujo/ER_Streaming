package DataStructures;

import java.util.HashSet;
import java.util.Set;

public class BlockString {
	private Set<Integer> blocks;
	private Integer entityId;
	private Boolean isSource;
	
	public BlockString() {
		super();
		this.blocks = new HashSet<Integer>();
	}
	
	public void merge(BlockString other) {
		if (other.getEntityId() != null) {
			setEntityId(other.getEntityId());
		}
		if (other.isSource() != null) {
			setSource(other.isSource());
		}
		blocks.addAll(other.getBlocks());
	}

	public Set<Integer> getBlocks() {
		return blocks;
	}

	public void setBlocks(Set<Integer> blocks) {
		this.blocks = blocks;
	}

	public Integer getEntityId() {
		return entityId;
	}

	public void setEntityId(int entityId) {
		this.entityId = entityId;
	}

	public Boolean isSource() {
		return isSource;
	}

	public void setSource(boolean isSource) {
		this.isSource = isSource;
	}
	
	public void addBlock(Integer idBlock) {
		blocks.add(idBlock);
	}
	
	
	
}
