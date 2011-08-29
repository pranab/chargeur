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

package org.chargeur.handler;

import org.chargeur.config.ColumnMapper;

public class ProductPreLoadHandler implements PreLoadHandler {
	
	public  String process(ColumnMapper colMapper, String value)  {
		String colValue = value;
		if (colMapper.getOrdinal() == 4){
			String[] items = value.split(":");
			
			if (colMapper.getCol().equals("title")){
				colValue = items[0];
			} else {
				if (items.length == 2){
					colValue = items[1];
				} else {
					colValue = colMapper.getDefaultValue();
				}
			}
		}
		return colValue;
		
	}

}
