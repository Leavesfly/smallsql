/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2006, by Volker Berlin.
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
 * ForeignKeys.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 23.04.2005
 */
package org.yf.smallsql.rdb.engine.table;
/**
 * A typed implementation of Arraylist for ForeignKey.
 * 
 * @author Volker Berlin
 *
 */
public class ForeignKeys {
	private int size;
	private ForeignKey[] data;
	
	
	public ForeignKeys(){
		data = new ForeignKey[16];
	}
	

	public final int size(){
		return size;
	}
	

	public final ForeignKey get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
		return data[idx];
	}
	
	
	public final void add(ForeignKey foreignKey){
		if(size >= data.length ){
			resize(size << 1);
		}
		data[size++] = foreignKey;
	}
	
	
	private final void resize(int newSize){
		ForeignKey[] dataNew = new ForeignKey[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
}
