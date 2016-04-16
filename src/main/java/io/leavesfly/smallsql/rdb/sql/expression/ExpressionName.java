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
 * ExpressionName.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.sql.expression;

import io.leavesfly.smallsql.rdb.engine.table.Column;
import io.leavesfly.smallsql.rdb.sql.Expression;
import io.leavesfly.smallsql.rdb.engine.View;
import io.leavesfly.smallsql.rdb.engine.selector.DataSource;
import io.leavesfly.smallsql.rdb.sql.datatype.MutableNumeric;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;

public class ExpressionName extends Expression {

	private String tableAlias;
	private DataSource fromEntry;
	private int colIdx;
	private View table;
	private Column column;

	// field name Expression i.e. abc, mytable.abc, "ab c"
	public ExpressionName(String name) {
		super(NAME);
		setName(name);
	}

	/**
	 * Constructor used for aggregate functions from parser.
	 * 
	 * @param type
	 *            the type of the aggregate function
	 */
	public ExpressionName(int type) {
		super(type);
		// setName( "" ); if null it will be generate a automatic name
	}

	public void setNameAfterTableAlias(String name) {
		tableAlias = getName();
		setName(name);
	}

	/**
	 * Is used in GroupResult.
	 */
	public boolean equals(Object expr) {
		if (!super.equals(expr))
			return false;
		if (!(expr instanceof ExpressionName))
			return false;
		if (((ExpressionName) expr).fromEntry != fromEntry)
			return false;
		return true;
	}

	public boolean isNull() throws Exception {
		return fromEntry.isNull(colIdx);
	}

	public boolean getBoolean() throws Exception {
		return fromEntry.getBoolean(colIdx);
	}

	public int getInt() throws Exception {
		return fromEntry.getInt(colIdx);
	}

	public long getLong() throws Exception {
		return fromEntry.getLong(colIdx);
	}

	public float getFloat() throws Exception {
		return fromEntry.getFloat(colIdx);
	}

	public double getDouble() throws Exception {
		return fromEntry.getDouble(colIdx);
	}

	public long getMoney() throws Exception {
		return fromEntry.getMoney(colIdx);
	}

	public MutableNumeric getNumeric() throws Exception {
		return fromEntry.getNumeric(colIdx);
	}

	public Object getObject() throws Exception {
		return fromEntry.getObject(colIdx);
	}

	public String getString() throws Exception {
		return fromEntry.getString(colIdx);
	}

	public byte[] getBytes() throws Exception {
		return fromEntry.getBytes(colIdx);
	}

	public int getDataType() {
		switch (getType()) {
		case NAME:
		case GROUP_BY:
			return fromEntry.getDataType(colIdx);
		case FIRST:
		case LAST:
		case MAX:
		case MIN:
		case SUM:
			return getParams()[0].getDataType();
		case COUNT:
			return SQLTokenizer.INT;
		default:
			throw new Error();
		}
	}

	/**
	 * Set the DataSource and the index in the DataSource. The first column has
	 * the index 0. The Table object is using to request the Column description.
	 */
	public void setFrom(DataSource fromEntry, int colIdx, View table) {
		this.fromEntry = fromEntry;
		this.colIdx = colIdx;
		this.table = table;
		// Because the DataSource is a TableResult the colIdx of both is
		// identical
		this.column = table.columns.get(colIdx);
	}

	/**
	 * Set the DataSource and the index in the DataSource. The first column has
	 * the index 0. The Table object is using to request the Column description.
	 */
	public void setFrom(DataSource fromEntry, int colIdx, Column column) {
		this.fromEntry = fromEntry;
		this.colIdx = colIdx;
		this.column = column;
	}

	public DataSource getDataSource() {
		return fromEntry;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	/**
	 * Get the table of this column
	 * 
	 * @return
	 */
	public final View getTable() {
		return table;
	}

	/**
	 * Get index of the column in the table
	 * 
	 * @return
	 */
	public final int getColumnIndex() {
		return colIdx;
	}

	public final Column getColumn() {
		return column;
	}

	final public String toString() {
		if (tableAlias == null)
			return String.valueOf(getAlias());
		return tableAlias + "." + getAlias();
	}

	/*
	 * =======================================================================
	 * 
	 * Methods for ResultSetMetaData
	 * 
	 * =======================================================================
	 */

	public String getTableName() {
		if (table != null) {
			return table.getName();
		}
		return null;
	}

	public int getPrecision() {
		return column.getPrecision();
	}

	public int getScale() {
		return column.getScale();
	}

	public int getDisplaySize() {
		return column.getDisplaySize();
	}

	public boolean isAutoIncrement() {
		return column.isAutoIncrement();
	}

	public boolean isCaseSensitive() {
		return column.isCaseSensitive();
	}

	public boolean isNullable() {
		return column.isNullable();
	}

	public boolean isDefinitelyWritable() {
		return true;
	}

}