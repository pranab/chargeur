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
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.chargeur.config.SpreadSheetMapper;

public class SolrIndexManager implements IndexManager {
	private CommonsHttpSolrServer server;
	private int batchSize;
	private int docCount;
	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private static final String idDelim = "[]";
	private static final String idFieldName = "id";

	public SolrIndexManager()  throws Exception {
		SpreadSheetMapper configurator = SpreadSheetMapper.instance();
		server = new CommonsHttpSolrServer(configurator.getIndexServerUrl());
		batchSize = configurator.getBatchSize();
	}
	
	@Override
	public void createIndex(List<ColumnValue> rowKeyValues,	List<ColumnValue> columns) throws Exception {
		SolrInputDocument doc = new SolrInputDocument();

		String id = createDocId(rowKeyValues);
		doc.addField(idFieldName, id);
		
		for (ColumnValue colVal : columns){
			if (colVal.isIndexed()){
				doc.addField(colVal.getName(), colVal.getTypedValue());
			}
		}
		
		docs.add(doc);
		++docCount;
		if (docCount % batchSize == 0){
			server.add(docs);
			server.commit();
			docs.clear();
			System.out.println("Index flushed");
		}
		
	}
	
	public void close() throws Exception {
		if (!docs.isEmpty()){
			server.add(docs);
			server.commit();
			System.out.println("Index flushed");
		}
	}
	
	private String createDocId(List<ColumnValue> rowKeyValues){
		StringBuilder stBld = new StringBuilder();
		int count = 0;
		for (ColumnValue colVal : rowKeyValues){
			if (count == 0){
				stBld.append(colVal.getValue());
			} else {
				stBld.append(idDelim).append(colVal.getValue());
			}
			++count;
		}
		
		return stBld.toString();
	}

}
