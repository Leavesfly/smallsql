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
 * ExpressionFunctionIIF.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 11.04.2004
 */
package org.yf.smallsql.rdb.sql.expression.function.system;

import org.yf.smallsql.rdb.sql.datatype.MutableNumeric;
import org.yf.smallsql.rdb.sql.expression.function.ExpressionFunction;
import org.yf.smallsql.rdb.sql.expression.operator.ExpressionArithmetic;
import org.yf.smallsql.rdb.sql.parser.SQLTokenizer;

/**
 * @author Volker Berlin
 */
public final class ExpressionFunctionIIF extends ExpressionFunction {

	public int getFunction() {
		return SQLTokenizer.IIF;
	}

	public boolean isNull() throws Exception {
		if (param1.getBoolean())
			return param2.isNull();
		return param3.isNull();
	}

	public boolean getBoolean() throws Exception {
		if (param1.getBoolean())
			return param2.getBoolean();
		return param3.getBoolean();
	}

	public int getInt() throws Exception {
		if (param1.getBoolean())
			return param2.getInt();
		return param3.getInt();
	}

	public long getLong() throws Exception {
		if (param1.getBoolean())
			return param2.getLong();
		return param3.getLong();
	}

	public float getFloat() throws Exception {
		if (param1.getBoolean())
			return param2.getFloat();
		return param3.getFloat();
	}

	public double getDouble() throws Exception {
		if (param1.getBoolean())
			return param2.getDouble();
		return param3.getDouble();
	}

	public long getMoney() throws Exception {
		if (param1.getBoolean())
			return param2.getMoney();
		return param3.getMoney();
	}

	public MutableNumeric getNumeric() throws Exception {
		if (param1.getBoolean())
			return param2.getNumeric();
		return param3.getNumeric();
	}

	public Object getObject() throws Exception {
		if (param1.getBoolean())
			return param2.getObject();
		return param3.getObject();
	}

	public String getString() throws Exception {
		if (param1.getBoolean())
			return param2.getString();
		return param3.getString();
	}

	public final int getDataType() {
		return ExpressionArithmetic.getDataType(param2, param3);
	}

	public final int getPrecision() {
		return Math.max(param2.getPrecision(), param3.getPrecision());
	}

	public final int getScale() {
		return Math.max(param2.getScale(), param3.getScale());
	}

}
