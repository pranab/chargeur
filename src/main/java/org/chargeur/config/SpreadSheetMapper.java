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

package org.chargeur.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

public class SpreadSheetMapper {
	private String name;
	private String database;
	private String table;
	private boolean colNameInFirstRow;
	private List<String> rowKey;
	private int batchSize;
	private String preLoadHandler;
	private String indexManagerClass;
	private String indexServerUrl;
	private List<ColumnMapper> columnMappers;    
	private static SpreadSheetMapper configurator;

    public static void initialize(String mappingFile) throws Exception{
        ObjectMapper mapper = new ObjectMapper(); 
        configurator = mapper.readValue(new File(mappingFile), SpreadSheetMapper.class);
    }
    
    public static SpreadSheetMapper instance(){
        return configurator;
    }

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public boolean isColNameInFirstRow() {
		return colNameInFirstRow;
	}

	public void setColNameInFirstRow(boolean colNameInFirstRow) {
		this.colNameInFirstRow = colNameInFirstRow;
	}

	public List<String> getRowKey() {
		return rowKey;
	}
	public void setRowKey(List<String> rowKey) {
		this.rowKey = rowKey;
	}
	public List<ColumnMapper> getColumnMappers() {
		return columnMappers;
	}
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public String getPreLoadHandler() {
		return preLoadHandler;
	}

	public void setPreLoadHandler(String preLoadHandler) {
		this.preLoadHandler = preLoadHandler;
	}

	public void setColumnMappers(List<ColumnMapper> columnMappers) {
		this.columnMappers = columnMappers;
	}
	
	public List<ColumnMapper> getColumnMapper(int colOrdinal){
		List<ColumnMapper> colMappers = new ArrayList<ColumnMapper>();
		
		for (ColumnMapper thisColMapper : columnMappers){
			if (thisColMapper.getOrdinal() == colOrdinal){
				colMappers.add(thisColMapper);
			}
		}
		return colMappers;
	}

	public List<ColumnMapper> getColumnMapperForSideData(){
		List<ColumnMapper> colMappers = new ArrayList<ColumnMapper>();
		
		for (ColumnMapper thisColMapper : columnMappers){
			if (thisColMapper.isSideData()){
				colMappers.add(thisColMapper);
			}
		}
		return colMappers;
	}

	public String getIndexManagerClass() {
		return indexManagerClass;
	}

	public void setIndexManagerClass(String indexManagerClass) {
		this.indexManagerClass = indexManagerClass;
	}

	public String getIndexServerUrl() {
		return indexServerUrl;
	}

	public void setIndexServerUrl(String indexServerUrl) {
		this.indexServerUrl = indexServerUrl;
	}
}
