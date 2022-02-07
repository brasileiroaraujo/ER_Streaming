package DataStructures;

public class TupleSimilarity implements Comparable<TupleSimilarity>{
	private Object key;
	private Double value;
	
	public TupleSimilarity(Integer key, Double value) {
		super();
		this.key = key;
		this.value = value;
	}
	public TupleSimilarity(String key, Double value) {
		super();
		this.key = key;
		this.value = value;
	}
	public Integer getKey() {
		return (Integer) key;
	}
	public String getKeyAsString() {
		return String.valueOf(key);
	}
	public void setKey(Integer key) {
		this.key = key;
	}
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	
	@Override
	public int compareTo(TupleSimilarity other) {
		if (other.getValue().compareTo(value) == 0) {
			if (key instanceof Integer && key.equals(other.getKey())) {
				return 0;
			} else if (key instanceof String && key.equals(other.getKeyAsString())) {
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
	
}
