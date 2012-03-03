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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ConsistencyLevel;

import agiato.cassandra.data.BatchLoader;
import agiato.cassandra.data.DataManager;
import agiato.cassandra.data.SuperColumnValue;
import agiato.cassandra.data.SuperRow;


public class CassandraLoader extends DbLoader {
	private BatchLoader batchLoader;

	public CassandraLoader(String configFile, String table, int batchSize) throws Exception {
		DataManager.initialize(configFile, false);
		BatchLoader batchLoader = new BatchLoader(table, batchSize, ConsistencyLevel.ONE);
	}
	
	
	@Override
	public void load(List<ColumnValue> rowKeyValues, List<ColumnValue> columns)
			throws Exception {
		 ByteBuffer rowKey = ByteBuffer.wrap( getCompositeKeyBytes(rowKeyValues, false));
		 List<SuperColumnValue> superColValues = new ArrayList<SuperColumnValue>();
		 Map<String, SuperColumnValue> superColsMap = new HashMap<String, SuperColumnValue>();
		 
		 for (ColumnValue colVal : columns ) {
			 //super col name and value
			 String supCol  = colVal.getColFamily();
			 SuperColumnValue supColVal = superColsMap.get(supCol);
			 if (null == supColVal) {
				 supColVal = new SuperColumnValue();
				 supColVal.setName(ByteBuffer.wrap(colVal.getColFamilyBytes()));
				 superColsMap.put(supCol, supColVal);
			 }
			 
			 agiato.cassandra.data.ColumnValue  caColVal = new agiato.cassandra.data.ColumnValue();
			 caColVal.setName(ByteBuffer.wrap(colVal.getColBytes()));
			 caColVal.setValue(ByteBuffer.wrap(colVal.getValueBytes()));
			 supColVal.addValue(caColVal);
		 }
		 
		 //create super col list and row
		 for (String name : superColsMap.keySet()) {
			 superColValues.add(superColsMap.get(name));
		 }
		SuperRow superRow = new  SuperRow(rowKey, superColValues);

		batchLoader.addRow(rowKey, superColValues);
	}

	@Override
	public void close() throws Exception {
		batchLoader.close();
	}

	@Override
	public int getRowCount() {
		return batchLoader.getRowCount();
	}

}
