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
 * ExpressionFunctionReturnFloat.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.sql.expression.function;

import io.leavesfly.smallsql.rdb.sql.datatype.MutableNumeric;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;
import io.leavesfly.smallsql.util.Utils;

public abstract class ExpressionFunctionReturnFloat extends ExpressionFunction {

	public boolean isNull() throws Exception {
		return param1.isNull();
	}

	public final boolean getBoolean() throws Exception {
		return getDouble() != 0;
	}

	public final int getInt() throws Exception {
		return (int) getDouble();
	}

	public final long getLong() throws Exception {
		return (long) getDouble();
	}

	public final float getFloat() throws Exception {
		return (float) getDouble();
	}

	public long getMoney() throws Exception {
		return Utils.doubleToMoney(getDouble());
	}

	public final MutableNumeric getNumeric() throws Exception {
		if (isNull())
			return null;
		double value = getDouble();
		if (Double.isInfinite(value) || Double.isNaN(value))
			return null;
		return new MutableNumeric(value);
	}

	public final Object getObject() throws Exception {
		if (isNull())
			return null;
		return new Double(getDouble());
	}

	public final String getString() throws Exception {
		Object obj = getObject();
		if (obj == null)
			return null;
		return obj.toString();
	}

	public final int getDataType() {
		return SQLTokenizer.FLOAT;
	}

}