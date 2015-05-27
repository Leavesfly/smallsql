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
 * ExpressionFunctionReturnP1StringAndBinary.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 17.06.2006
 */
package org.yf.smallsql.rdb.sql.expression.function;

import org.yf.smallsql.jdbc.metadata.SsResultSetMetaData;
import org.yf.smallsql.rdb.sql.datatype.Money;
import org.yf.smallsql.rdb.sql.datatype.MutableNumeric;
import org.yf.smallsql.util.Utils;

/**
 * Super Class for String and binary functions which extends the metadata from
 * the first parameter.
 * 
 * @author Volker Berlin
 */
public abstract class ExpressionFunctionReturnP1StringAndBinary extends ExpressionFunctionReturnP1 {

	public final boolean getBoolean() throws Exception {
		if (isNull())
			return false;
		return Utils.string2boolean(getString().trim());
	}

	public final int getInt() throws Exception {
		if (isNull())
			return 0;
		return Integer.parseInt(getString().trim());
	}

	public final long getLong() throws Exception {
		if (isNull())
			return 0;
		return Long.parseLong(getString().trim());
	}

	public final float getFloat() throws Exception {
		if (isNull())
			return 0;
		return Float.parseFloat(getString().trim());
	}

	public final double getDouble() throws Exception {
		if (isNull())
			return 0;
		return Double.parseDouble(getString().trim());
	}

	public final long getMoney() throws Exception {
		if (isNull())
			return 0;
		return Money.parseMoney(getString().trim());
	}

	public final MutableNumeric getNumeric() throws Exception {
		if (isNull())
			return null;
		return new MutableNumeric(getString().trim());
	}

	public final Object getObject() throws Exception {
		if (SsResultSetMetaData.isBinaryDataType(param1.getDataType()))
			return getBytes();
		return getString();
	}
}
