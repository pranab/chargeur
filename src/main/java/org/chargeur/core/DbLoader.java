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


import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class DbLoader {

	public abstract void load(List<ColumnValue> rowKeyValues, List<ColumnValue> columns) throws Exception;
	
	public abstract void close() throws Exception;
	
	public abstract int getRowCount();
	
	protected   byte[] getCompositeKeyBytes(List<ColumnValue> rowKeyValues, boolean paddedString) {
		byte[] rowKey = null;
		int size = 0;
		for (ColumnValue colVal : rowKeyValues){
			size += colVal.getSize(paddedString);
		}

		rowKey = new byte[size];
		int tgtOffset = 0;
		for (ColumnValue colVal : rowKeyValues){
			int srcLength = colVal.getSize(paddedString);
			byte[] srcBytes = paddedString ? colVal.getValueBytesMax() : colVal.getValueBytes();
			tgtOffset = Bytes.putBytes(rowKey, tgtOffset, srcBytes, 0, srcLength);
		}
		
		return rowKey;
	}

}