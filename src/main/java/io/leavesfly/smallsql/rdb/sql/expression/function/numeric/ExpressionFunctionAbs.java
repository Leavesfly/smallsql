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
 * ExpressionFunctionAbs.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.sql.expression.function.numeric;

import io.leavesfly.smallsql.rdb.sql.datatype.Money;
import io.leavesfly.smallsql.rdb.sql.datatype.MutableNumeric;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;
import io.leavesfly.smallsql.rdb.sql.expression.function.ExpressionFunctionReturnP1;

/**
 * ABS函数实现类，用于计算数值的绝对值
 * 
 * 使用了策略模式:
 * - 继承ExpressionFunctionReturnP1类，提供特定于ABS函数的实现
 * - 通过getFunction()方法标识具体函数类型
 * - 根据不同数据类型提供相应的绝对值计算方法
 */
public class ExpressionFunctionAbs extends ExpressionFunctionReturnP1 {

	public int getFunction() {
		return SQLTokenizer.ABS;
	}

	public boolean getBoolean() throws Exception {
		return getDouble() != 0;
	}

	public int getInt() throws Exception {
		return Math.abs(param1.getInt());
	}

	public long getLong() throws Exception {
		return Math.abs(param1.getLong());
	}

	public float getFloat() throws Exception {
		return Math.abs(param1.getFloat());
	}

	public double getDouble() throws Exception {
		return Math.abs(param1.getDouble());
	}

	public long getMoney() throws Exception {
		return Math.abs(param1.getMoney());
	}

	public MutableNumeric getNumeric() throws Exception {
		if (param1.isNull())
			return null;
		MutableNumeric num = param1.getNumeric();
		if (num.getSignum() < 0)
			num.setSignum(1);
		return num;
	}

	public Object getObject() throws Exception {
		if (param1.isNull())
			return null;
		Object para1 = param1.getObject();
		switch (param1.getDataType()) {
		case SQLTokenizer.FLOAT:
		case SQLTokenizer.DOUBLE:
			double dValue = ((Double) para1).doubleValue();
			return (dValue < 0) ? new Double(-dValue) : para1;
		case SQLTokenizer.REAL:
			double fValue = ((Float) para1).floatValue();
			return (fValue < 0) ? new Float(-fValue) : para1;
		case SQLTokenizer.BIGINT:
			long lValue = ((Number) para1).longValue();
			return (lValue < 0) ? new Long(-lValue) : para1;
		case SQLTokenizer.TINYINT:
		case SQLTokenizer.SMALLINT:
		case SQLTokenizer.INT:
			int iValue = ((Number) para1).intValue();
			return (iValue < 0) ? new Integer(-iValue) : para1;
		case SQLTokenizer.NUMERIC:
		case SQLTokenizer.DECIMAL:
			MutableNumeric nValue = (MutableNumeric) para1;
			if (nValue.getSignum() < 0)
				nValue.setSignum(1);
			return nValue;
		case SQLTokenizer.MONEY:
			Money mValue = (Money) para1;
			if (mValue.value < 0)
				mValue.value = -mValue.value;
			return mValue;
		default:
			throw createUnspportedDataType(param1.getDataType());
		}
	}

	public String getString() throws Exception {
		Object obj = getObject();
		if (obj == null)
			return null;
		return obj.toString();
	}

}