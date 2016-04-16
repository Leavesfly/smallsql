/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2007, by Volker Berlin.
 *
 * Project Info:  http://www.smallsql.de/
 *
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by 
 * the Free Software Foundation; either version 2.1 of the License, or 
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but 
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, 
 * USA.  
 *
 * [Java is a trademark or registered trademark of Sun Microsystems, Inc. 
 * in the United States and other countries.]
 *
 * ---------------
 * IndexDescriptions.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 23.04.2005
 */
package io.leavesfly.smallsql.rdb.engine.index;

import java.sql.SQLException;

import io.leavesfly.smallsql.rdb.sql.datatype.Strings;
import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.rdb.engine.Database;
import io.leavesfly.smallsql.rdb.engine.View;

/**
 * A typed implementation of Arraylist for IndexDescription.
 * 
 * @author Volker Berlin
 *
 */
public class IndexDescriptions {
	private int size;
	private IndexDescription[] data;
    /** Only one primary Key per Table are valid */
    private boolean hasPrimary;
	
	
	public IndexDescriptions(){
		data = new IndexDescription[4];
	}
	

	public final int size(){
		return size;
	}
	

	public final IndexDescription get(int idx){
		// SAVER: use SmallSQLException
		if (idx >= size)
			throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
		return data[idx];
	}
	
	
	public final void add(IndexDescription descr) throws SQLException{
		if(size >= data.length ){
			resize(size << 1);
		}
        if(hasPrimary && descr.isPrimary()){
            throw SmallSQLException.create(Language.PK_ONLYONE);
        }
        hasPrimary = descr.isPrimary();
		data[size++] = descr;
	}
	
	
	private final void resize(int newSize){
		IndexDescription[] dataNew = new IndexDescription[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
	
	
	final IndexDescription findBestMatch(Strings columns){
		int bestFactor = Integer.MAX_VALUE;
		int bestIdx = 0;
		for(int i=0; i<size; i++){
			int factor = data[i].matchFactor(columns);
			if(factor == 0) 
				return data[i];
			
			if(factor < bestFactor){
				bestFactor = factor;
				bestIdx = i;
			}
		}
		if(bestFactor == Integer.MAX_VALUE)
			return null;
		else
			return data[bestIdx];
	}


	/**
	 * Create all the indexes. This means a file for every index is save.
	 * @param database 
	 * @param tableView
	 * @see IndexDescription#setTableView
	 */
	public void create(SsConnection con, Database database, View tableView) throws Exception{
		for(int i=0; i<size; i++){
			data[i].create(con, database, tableView);
		}
	}


	public void drop(Database database) throws Exception {
		for(int i=0; i<size; i++){
			data[i].drop(database);
		}
	}


    public void close() throws Exception{
        for(int i=0; i<size; i++){
            data[i].close();
        }
    }


    public void add(IndexDescriptions indexes) throws SQLException {
        for(int i=0; i<indexes.size; i++){
            add(indexes.data[i]);
        }
    }
}
