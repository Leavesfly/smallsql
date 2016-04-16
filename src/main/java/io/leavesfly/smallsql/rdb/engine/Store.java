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
 * Store.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.engine;

import java.sql.*;

import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.rdb.engine.store.StoreNoCurrentRow;
import io.leavesfly.smallsql.rdb.sql.datatype.MutableNumeric;
import io.leavesfly.smallsql.rdb.engine.store.StoreNull;

/**
 * @author Volker Berlin
 *
 */
public abstract class Store {

	public static final Store NULL = new StoreNull();
	public static final Store NOROW = new StoreNoCurrentRow();

	public abstract boolean isNull(int offset) throws Exception;

	public abstract boolean getBoolean(int offset, int dataType)
			throws Exception;

	public abstract byte[] getBytes(int offset, int dataType) throws Exception;

	public abstract double getDouble(int offset, int dataType) throws Exception;

	public abstract float getFloat(int offset, int dataType) throws Exception;

	public abstract int getInt(int offset, int dataType) throws Exception;

	public abstract long getLong(int offset, int dataType) throws Exception;

	public abstract long getMoney(int offset, int dataType) throws Exception;

	public abstract MutableNumeric getNumeric(int offset, int dataType)
			throws Exception;

	public abstract Object getObject(int offset, int dataType) throws Exception;

	public abstract String getString(int offset, int dataType) throws Exception;

	/**
	 * Get the status of the current page.
	 * 
	 * @return true if the current row valid. false if deleted or updated data.
	 */
	public boolean isValidPage() {
		return false;
	}

	public abstract void scanObjectOffsets(int[] offsets, int dataTypes[]);

	public abstract int getUsedSize();

	public abstract long getNextPagePos();

	public abstract void deleteRow(SsConnection con) throws SQLException;
}