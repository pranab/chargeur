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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseLoader implements DbLoader {
	private Configuration conf;
	private HTable table;
	private int batchSize;
	private int rowCount;
	
	public HbaseLoader(String tableName, int batchSize) throws Exception{
		conf = HBaseConfiguration.create();
		table = new HTable(conf, tableName);
		table.setAutoFlush(false);
		this.batchSize = batchSize;
	}
	
	/* (non-Javadoc)
	 * @see org.chargeur.core.DbLoader#load(java.lang.String, java.util.List)
	 */
	public void load(List<ColumnValue> rowKeyValues, List<ColumnValue> columns) throws Exception{
		byte[] rowKey =createRowKey(rowKeyValues);
		List<Put> puts = new ArrayList<Put>();
		
		for (ColumnValue colVal : columns){
			Put put = new Put(rowKey);
			put.add(colVal.getColFamilyBytes(), colVal.getColBytes(), colVal.getValueBytes());
			puts.add(put);
		}
		
		table.put(puts);
		++rowCount;
		
		if (rowCount % batchSize == 0){
			table.flushCommits();
			System.out.println("table flushed");
		}
	}
	
	public void close() throws Exception {
		if (rowCount % batchSize != 0){
			table.flushCommits();
			System.out.println("table flushed");
		}
		table.close();
		System.out.println("table closed");
	}

	public int getRowCount() {
		return rowCount;
	}
	
	public static  byte[] createRowKey(List<ColumnValue> rowKeyValues) {
		byte[] rowKey = null;
		int size = 0;
		for (ColumnValue colVal : rowKeyValues){
			size += colVal.getMaxSize();
		}

		rowKey = new byte[size];
		int tgtOffset = 0;
		for (ColumnValue colVal : rowKeyValues){
			int srcLength = colVal.getMaxSize();
			byte[] srcBytes = colVal.getValueBytesMax();
			Bytes.putBytes(rowKey, tgtOffset, srcBytes, 0, srcLength);
			tgtOffset += srcLength;
		}
		
		return rowKey;
	}

}
