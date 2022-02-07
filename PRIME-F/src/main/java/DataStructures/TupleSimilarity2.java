package DataStructures;

public class TupleSimilarity2 implements Comparable<TupleSimilarity2>{
	private Long key;
	private Double value;
	private String text;
	
	public TupleSimilarity2(Long key, Double value, String text) {
		super();
		this.key = key;
		this.value = value;
		this.text = text;
	}
	public Long getKey() {
		return key;
	}
	public void setKey(Long key) {
		this.key = key;
	}
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	
	@Override
	public int compareTo(TupleSimilarity2 other) {
		if (key.equals(other.getKey())) {
			return 0;
		}
		
		if (other.getValue().compareTo(value) == 0) {
			if (key.equals(other.getKey())) {
				return 0;
			} else {
				return 1;
			}
		}
		
		return other.getValue().compareTo(value);
	}
	
	@Override
	public String toString() {
		return key + ": " + value;
	}
	
//	@Override
//	public boolean equals(Object obj) {
//		if (obj instanceof TupleSimilarity2) {
//			return key.equals(((TupleSimilarity2)obj).getKey());
//		}
//		return false;
//	}
	
	@Override
	public int hashCode() {
		return key.hashCode();
	}
	
}
