/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2011, by Volker Berlin.
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
 * Column.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.engine.table;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;

import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.jdbc.metadata.SsResultSetMetaData;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.rdb.sql.Expression;
import io.leavesfly.smallsql.rdb.sql.datatype.Identity;
import io.leavesfly.smallsql.rdb.sql.expression.ExpressionValue;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;

public class Column implements Cloneable {

	// private Expression value;
	private Expression defaultValue = Expression.NULL; // Default value for
														// INSERT
	private String defaultDefinition; // String representation for Default Value
	private String name;
	private boolean identity;
	private boolean caseSensitive;
	private boolean nullable = true;
	private int scale;
	private int precision;
	private int dataType;
	private Identity counter; // counter for identity values

	public void setName(String name) {
		this.name = name;
	}

	public void setDefaultValue(Expression defaultValue,
			String defaultDefinition) {
		this.defaultValue = defaultValue;
		this.defaultDefinition = defaultDefinition;
	}

	/**
	 * Return the default expression for this column. If there is no default
	 * vale then it return Expression.NULL.
	 * 
	 * @param con
	 *            SSConnection for transactions
	 */
	public Expression getDefaultValue(SsConnection con) throws SQLException {
		if (identity)
			counter.createNextValue(con);
		return defaultValue;
	}

	public String getDefaultDefinition() {
		return defaultDefinition;
	}

	public String getName() {
		return name;
	}

	public boolean isAutoIncrement() {
		return identity;
	}

	public void setAutoIncrement(boolean identity) {
		this.identity = identity;
	}

	public int initAutoIncrement(FileChannel raFile, long filePos)
			throws IOException {
		if (identity) {
			counter = new Identity(raFile, filePos);
			defaultValue = new ExpressionValue(counter, SQLTokenizer.BIGINT);
		}
		return 8;
	}

	public void setNewAutoIncrementValue(Expression obj) throws Exception {
		if (identity) {
			counter.setNextValue(obj);
		}
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public int getDataType() {
		return dataType;
	}

	public int getDisplaySize() {
		return SsResultSetMetaData.getDisplaySize(dataType, precision, scale);
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public int getScale() {
		switch (dataType) {
		case SQLTokenizer.DECIMAL:
		case SQLTokenizer.NUMERIC:
			return scale;
		default:
			return Expression.getScale(dataType);
		}
	}

	public void setPrecision(int precision) throws SQLException {
		if (precision < 0)
			throw SmallSQLException.create(Language.COL_INVALID_SIZE,
					new Object[] { new Integer(precision), name });
		this.precision = precision;
	}

	public int getPrecision() {
		return SsResultSetMetaData.getDataTypePrecision(dataType, precision);
	}

	public int getColumnSize() {
		if (SsResultSetMetaData.isNumberDataType(dataType))
			return getPrecision();
		else
			return getDisplaySize();
	}

	public int getFlag() {
		return (identity ? 1 : 0) | (caseSensitive ? 2 : 0)
				| (nullable ? 4 : 0);
	}

	public void setFlag(int flag) {
		identity = (flag & 1) > 0;
		caseSensitive = (flag & 2) > 0;
		nullable = (flag & 4) > 0;
	}

	public Column copy() {
		try {
			return (Column) clone();
		} catch (Exception e) {
			return null;
		}

	}
}