
package org.chargeur.config;

public class ColumnMapper {
	private String name;
	private int ordinal;
	private String dataType = "string";
	private String colFamily;
	private String col;
	private boolean useAsRowKey = false;
	private int maxSize;
	private boolean sideData = false;
	private String defaultValue;
	private boolean manyToOne;
	private boolean indexed;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getOrdinal() {
		return ordinal;
	}
	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getColFamily() {
		return colFamily;
	}
	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}
	public String getCol() {
		return col;
	}
	public void setCol(String col) {
		this.col = col;
	}
	public boolean isUseAsRowKey() {
		return useAsRowKey;
	}
	public void setUseAsRowKey(boolean useAsRowKey) {
		this.useAsRowKey = useAsRowKey;
	}
	public int getMaxSize() {
		return maxSize;
	}
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	public boolean isSideData() {
		return sideData;
	}
	public void setSideData(boolean sideData) {
		this.sideData = sideData;
	}
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	public boolean isManyToOne() {
		return manyToOne;
	}
	public void setManyToOne(boolean manyToOne) {
		this.manyToOne = manyToOne;
	}
	public boolean isIndexed() {
		return indexed;
	}
	public void setIndexed(boolean indexed) {
		this.indexed = indexed;
	}
	
	
}
