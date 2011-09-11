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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.chargeur.config.ColumnMapper;
import org.chargeur.config.SpreadSheetMapper;
import org.chargeur.handler.PreLoadHandler;


/**
 *
 * @author Pranab
 */

public class Loader {
	private SpreadSheetMapper configurator;
	private PreLoadHandler handler;
	private IndexManager indexManager;
	
	public void load(String csvFile, String mappingFile, Map<String, String> sideData){
		try {
			SpreadSheetMapper.initialize(mappingFile);
			configurator = SpreadSheetMapper.instance();
			
			//create loader object
			DbLoader dbLoader = createDbLoader();
			
			//create pre load handler object
			String preLoadHandler = configurator.getPreLoadHandler();
			handler = null;
			if (null != preLoadHandler){
	            Class<?> handlerCls = Class.forName(preLoadHandler);
	            handler = (PreLoadHandler)handlerCls.newInstance();
			}
			
			//create index manager object
			String indexManagerClass = configurator.getIndexManagerClass();
			if (null != indexManagerClass) {
	            Class<?> indexManCls = Class.forName(indexManagerClass);
	            indexManager = (IndexManager)indexManCls.newInstance();
			}
			
			List<ColumnValue> columns = new ArrayList<ColumnValue>();
			Map<String, ColumnValue> rowComponents = new HashMap<String, ColumnValue>();
			
			FileInputStream fstream = new FileInputStream(csvFile);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			
			//process each line
			boolean firstTime = true;
            int count = 0;
			while ((line = br.readLine()) != null) {
				if (firstTime && configurator.isColNameInFirstRow()) {
					firstTime = false;
					continue;
				}
				if (line.isEmpty()){
					continue;
				}
				
				System.out.println("processing row: " + (count+1));
				
				columns.clear();
				List<ColumnValue> rowKeyValues = null;
				rowComponents.clear();
				
				//System.out.println(line);
				line = line.replaceAll("\"", "");
				System.out.println("\n\n" + line);
						
				String[] items = line.split(",");
				System.out.println("num columns " + items.length);
				
				for (int i = 0; i < items.length; ++i){
					String value = items[i];
					value = value.trim();
					
					//System.out.println("processing column: " + (i+1) + "  " + value);
					List<ColumnMapper> colMappers = configurator.getColumnMapper(i+1);
					for (ColumnMapper colMapper : colMappers){
						createColumnValue(value, colMapper,  columns, rowComponents);
					}
				}
				
				//side data
				List<ColumnMapper> colMappersSideData = configurator.getColumnMapperForSideData();
				for (ColumnMapper colMapper : colMappersSideData){
					String value = sideData.get(colMapper.getName());
					if (null != value){
						//System.out.println("processing side data: " + colMapper.getName());
						createColumnValue(value, colMapper,  columns, rowComponents);
					}
				}
				
				
				
				//load this row
	            ++count;
				rowKeyValues = getRowKeyItems(rowComponents);
				dbLoader.load(rowKeyValues, columns);
				System.out.println("Loaded row: " + count);
				
				//create index
				if (null != indexManager){
					indexManager.createIndex(rowKeyValues, columns);
					System.out.println("Indexed row: " + count);
				}
 				
			}
			
			in.close();			
			dbLoader.close();
			
			if (null != indexManager){
				indexManager.close();
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err.println("Error loading.." + ex);
		}
		
	}
	
	private DbLoader createDbLoader() throws Exception {
		DbLoader dbLoader = null;
		
		if (configurator.getDatabase().equals("hbase")){
			dbLoader = new HbaseLoader(configurator.getTable(), configurator.getBatchSize());
		}
		
		return dbLoader;
	}
	
	private List<ColumnValue> getRowKeyItems(Map<String, ColumnValue> rowComponents) {
		List<String> rowKeyItems = configurator.getRowKey();
		List<ColumnValue> rowKeyValues = new ArrayList<ColumnValue>();
		for (String rowKeyItem : rowKeyItems){
			ColumnValue colVal = rowComponents.get(rowKeyItem);
			System.out.println("creating row key: " + rowKeyItem + " " + colVal.getValue());
			
			rowKeyValues.add(colVal);
		}
		
		return rowKeyValues;
	}
	
	private ColumnValue findColumnValue(List<ColumnValue> columns, ColumnMapper colMapper){
		ColumnValue colValue = null;
		
		for (ColumnValue thisColValue : columns) {
			if (thisColValue.getColFamily().equals(colMapper.getColFamily()) && 
					thisColValue.getCol().equals(colMapper.getCol())) {
				colValue = thisColValue;
				break;
			}
		}
		
		return colValue;
	}
	
	private void createColumnValue(String value, ColumnMapper colMapper, List<ColumnValue> columns, Map<String, ColumnValue> rowComponents){
		String processedValue = handler != null ? handler.process(colMapper, value) : value;
		
		boolean exists = false;
		ColumnValue colValue = findColumnValue(columns, colMapper);
		if (null != colValue){
			colValue.appendValue(processedValue);
			exists = true;
		} else {
			colValue = new ColumnValue(colMapper, processedValue);
		}
		
		if (colMapper.isUseAsRowKey()){
			rowComponents.put(colMapper.getName(), colValue);
			System.out.println("row key: " + colMapper.getName() + " " + colValue.getValue());
		} else {
			if (!exists){
				columns.add(colValue);
			}
		}
	}

    public static void main(String[] args){
    	Loader loader = new Loader();
    	Map<String, String> sideData = new HashMap<String, String>();
    	sideData.put("seller", "testSeller");
    	loader.load(args[0], args[1], sideData);
	}
}
