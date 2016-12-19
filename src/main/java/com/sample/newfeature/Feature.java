package com.sample.newfeature;

public class Feature implements Comparable<Feature>{
	private String value;

	private Long index;

	public Feature(Long index) {
		super();
		this.index = index;
	}
	
	public Feature(Integer index) {
		super();
		this.index = new Long(index);
	}
	
	public Feature(String value, Long index) {
		super();
		this.value = value;
		this.index = index;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Long getIndex() {
		return index;
	}

	public void setIndex(Long index) {
		this.index = index;
	}

	@Override
	public int compareTo(Feature o) {
		if(this.getIndex() == o.getIndex())
			return 0;
		else
			return new Long( this.getIndex() - o.getIndex() ).intValue();
	}

}
