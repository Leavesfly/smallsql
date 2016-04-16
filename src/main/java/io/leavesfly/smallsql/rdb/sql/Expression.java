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
 * Expression.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.sql;

import java.sql.SQLException;

import io.leavesfly.smallsql.jdbc.metadata.SsResultSetMetaData;
import io.leavesfly.smallsql.rdb.sql.datatype.Mutable;
import io.leavesfly.smallsql.rdb.sql.datatype.MutableNumeric;
import io.leavesfly.smallsql.rdb.sql.expression.ExpressionValue;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;

public abstract class Expression implements Cloneable {

	public static final Expression NULL = new ExpressionValue(null, SQLTokenizer.NULL);

	final private int type;
	private String name; // the name of the original column in the table
	private String alias;

	/**
	 * A list of parameters. It is used for ExpressionFunction and
	 * ExpressionAritmethik. Do not modify this variable from extern directly
	 * because there are other references. Use the methods setParams() and
	 * setParamAt()
	 * 
	 * @see #setParams
	 * @see setParamAt
	 */
	private Expression[] params;

	protected Expression(int type) {
		this.type = type;
	}

	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public final String getName() {
		return name;
	}

	public final void setName(String name) {
		this.alias = this.name = name;
	}

	public final String getAlias() {
		return alias;
	}

	public final void setAlias(String alias) {
		this.alias = alias;
	}

	public void setParams(Expression[] params) {
		this.params = params;
	}

	/**
	 * Replace the idx parameter. You need to use this method to modify the
	 * <code>params</code> array because there there can be other references to
	 * the <code>params</code>.
	 */
	public void setParamAt(Expression param, int idx) {
		params[idx] = param;
	}

	public final Expression[] getParams() {
		return params;
	}

	/**
	 * Optimize the expression after a command was compiled. This can be
	 * constant expressions that are evaluate once.
	 * 
	 * @throws SQLException
	 */
	public void optimize() throws SQLException {
		if (params != null) {
			for (int p = 0; p < params.length; p++) {
				params[p].optimize();
			}
		}
	}

	/**
	 * Is used in GroupResult.
	 */
	public boolean equals(Object expr) {
		if (!(expr instanceof Expression))
			return false;
		if (((Expression) expr).type == type) {

			Expression[] p1 = ((Expression) expr).params;
			Expression[] p2 = params;
			
			if (p1 != null && p2 != null) {
				for (int i = 0; i < p1.length; i++) {
					if (!p2[i].equals(p1[i]))
						return false;
				}
			}
			String name1 = ((Expression) expr).name;
			String name2 = name;
			if (name1 == name2)
				return true;
			if (name1 == null)
				return false;
			if (name1.equalsIgnoreCase(name2))
				return true;
		}
		return false;
	}

	public abstract boolean isNull() throws Exception;

	public abstract boolean getBoolean() throws Exception;

	public abstract int getInt() throws Exception;

	public abstract long getLong() throws Exception;

	public abstract float getFloat() throws Exception;

	public abstract double getDouble() throws Exception;

	public abstract long getMoney() throws Exception;

	public abstract MutableNumeric getNumeric() throws Exception;

	public abstract Object getObject() throws Exception;

	public final Object getApiObject() throws Exception {
		Object obj = getObject();
		if (obj instanceof Mutable) {
			return ((Mutable) obj).getImmutableObject();
		}
		return obj;
	}

	public abstract String getString() throws Exception;

	public abstract byte[] getBytes() throws Exception;

	public abstract int getDataType();

	public final int getType() {
		return type;
	}

	/*
	 * =======================================================================
	 * 
	 * Methods for ResultSetMetaData
	 * 
	 * =======================================================================
	 */

	public String getTableName() {
		return null;
	}

	public int getPrecision() {
		return SsResultSetMetaData.getDataTypePrecision(getDataType(), -1);
	}

	public int getScale() {
		return getScale(getDataType());
	}

	public final static int getScale(int dataType) {
		switch (dataType) {
		case SQLTokenizer.MONEY:
		case SQLTokenizer.SMALLMONEY:
			return 4;
		case SQLTokenizer.TIMESTAMP:
			return 9; // nano seconds
		case SQLTokenizer.NUMERIC:
		case SQLTokenizer.DECIMAL:
			return 38;
		default:
			return 0;
		}
	}

	public int getDisplaySize() {
		return SsResultSetMetaData.getDisplaySize(getDataType(), getPrecision(), getScale());
	}

	public boolean isDefinitelyWritable() {
		return false;
	}

	public boolean isAutoIncrement() {
		return false;
	}

	public boolean isCaseSensitive() {
		return false;
	}

	public boolean isNullable() {
		return true;
	}

	public static final int VALUE = 1;
	public static final int NAME = 2;
	public static final int FUNCTION = 3;
	public static final int GROUP_BY = 11;
	public static final int COUNT = 12;
	public static final int SUM = 13;
	public static final int FIRST = 14;
	public static final int LAST = 15;
	public static final int MIN = 16;
	public static final int MAX = 17;
	public static final int GROUP_BEGIN = GROUP_BY;

}