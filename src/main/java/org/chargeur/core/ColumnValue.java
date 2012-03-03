/*
 * Chargeur: Loads HBase from csv
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.chargeur.core;

import org.apache.hadoop.hbase.util.Bytes;
import org.chargeur.config.ColumnMapper;

public class ColumnValue {
	private String name;
	private String colFamily;
	private String col;
	private String value;
	private String dataType;
	private int maxSize;
	private boolean indexed;
	
	public ColumnValue(ColumnMapper colMapper, String value){
		if (colMapper.isManyToOne()){
			name = colMapper.getCol();
		} else {
			name = colMapper.getName();
		}
		
		colFamily = colMapper.getColFamily();
		col = colMapper.getCol();
		dataType = colMapper.getDataType();
		maxSize = colMapper.getMaxSize();
		
		if (value.isEmpty() && null != colMapper.getDefaultValue()){
			value = colMapper.getDefaultValue();
		}
		
		if (dataType.equals("long")) {
			maxSize = Bytes.SIZEOF_LONG;
		} else if (dataType.equals("double")) {
			maxSize = Bytes.SIZEOF_DOUBLE;
		}
		this.value = value;
		this.indexed = colMapper.isIndexed();
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public int getMaxSize() {
		return maxSize;
	}
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	
	public byte[] getColFamilyBytes(){
		return Bytes.toBytes(colFamily);
	}

	public byte[] getColBytes(){
		return Bytes.toBytes(col);
	}

	public byte[] getValueBytes() {
		byte[] bytes = null;
		
		if (dataType.equals("string") || dataType.equals("text")) {
			bytes = Bytes.toBytes(value);
		} else if (dataType.equals("long")) { 
			long lValue = new Long(value);
			bytes = Bytes.toBytes(lValue);
		} else if (dataType.equals("double")) {
			double dValue = new Double(value);
			bytes = Bytes.toBytes(dValue);
		}
		return bytes;
	}

	public byte[] getValueBytesMax() {
		byte[] bytes = null;
		
		if (dataType.equals("string") || dataType.equals("text")) {
			byte[] stBytes = Bytes.toBytes(value);
			if (stBytes.length < maxSize){
				bytes = Bytes.padTail(stBytes, maxSize - stBytes.length);
			} else {
				bytes = Bytes.head(stBytes, maxSize);
			}
		} else if (dataType.equals("long")) { 
			long lValue = new Long(value);
			bytes = Bytes.toBytes(lValue);
		} else if (dataType.equals("double")) {
			double dValue = new Double(value);
			bytes = Bytes.toBytes(dValue);
		}
		return bytes;
	}
	
	public Object getTypedValue() {
		Object typedValue = null;
		
		if (dataType.equals("string") || dataType.equals("text")) {
			typedValue = value;
		} else if (dataType.equals("long")) { 
			typedValue = new Long(value);
		} else if (dataType.equals("double")) {
			typedValue = new Double(value);
		}
		return typedValue;
	}

	public void appendValue(String newValue){
		value = value + " " + newValue;
	}

	public boolean isIndexed() {
		return indexed;
	}

	public void setIndexed(boolean indexed) {
		this.indexed = indexed;
	}
	
	public int getSize(boolean getMax) {
		int size = 0;
		if (dataType.equals("string") || dataType.equals("text")) {
			size = getMax ? maxSize  :  value.length();
		} else if (dataType.equals("long")) { 
			size = Bytes.SIZEOF_LONG;
		} else if (dataType.equals("double")) {
			size = Bytes.SIZEOF_DOUBLE;
		}
		return size;
	}
}
