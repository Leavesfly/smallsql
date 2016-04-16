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
 * ExpressionFunctionCase.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 29.06.2004
 */
package io.leavesfly.smallsql.rdb.sql.expression.function.system;

import io.leavesfly.smallsql.rdb.sql.Expression;
import io.leavesfly.smallsql.rdb.sql.datatype.MutableNumeric;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;
import io.leavesfly.smallsql.rdb.sql.expression.Expressions;
import io.leavesfly.smallsql.rdb.sql.expression.operator.ExpressionArithmetic;

/**
 * @author Volker Berlin
 */
public final class ExpressionFunctionCase extends Expression/* Function */{

	/**
	 * @param type
	 */
	public ExpressionFunctionCase() {
		super(FUNCTION);
	}

	private final Expressions cases = new Expressions();
	private final Expressions results = new Expressions();
	private Expression elseResult = Expression.NULL;
	private int dataType = -1;

	public final void addCase(Expression condition, Expression result) {
		cases.add(condition);
		results.add(result);
	}

	public final void setElseResult(Expression expr) {
		elseResult = expr;
	}

	/**
	 * The structure is finish
	 */
	public final void setEnd() {
		Expression[] params = new Expression[cases.size() * 2
				+ (elseResult != null ? 1 : 0)];
		int i = 0;
		for (int p = 0; p < cases.size(); p++) {
			params[i++] = cases.get(p);
			params[i++] = results.get(p);
		}
		if (i < params.length)
			params[i] = elseResult;
		super.setParams(params);
	}

	public final void setParams(Expression[] params) {
		super.setParams(params);
		int i = 0;
		for (int p = 0; p < cases.size(); p++) {
			cases.set(p, params[i++]);
			results.set(p, params[i++]);
		}
		if (i < params.length)
			elseResult = params[i];
	}

	public void setParamAt(Expression param, int idx) {
		super.setParamAt(param, idx);
		int p = idx / 2;
		if (p >= cases.size()) {
			elseResult = param;
			return;
		}
		if (idx % 2 > 0) {
			results.set(p, param);
		} else {
			cases.set(p, param);
		}
	}

	// ================================
	// Methods of the interface
	// ================================

	final int getFunction() {
		return SQLTokenizer.CASE;
	}

	public final boolean isNull() throws Exception {
		return getResult().isNull();
	}

	public final boolean getBoolean() throws Exception {
		return getResult().getBoolean();
	}

	public final int getInt() throws Exception {
		return getResult().getInt();
	}

	public final long getLong() throws Exception {
		return getResult().getLong();
	}

	public final float getFloat() throws Exception {
		return getResult().getFloat();
	}

	public final double getDouble() throws Exception {
		return getResult().getDouble();
	}

	public final long getMoney() throws Exception {
		return getResult().getMoney();
	}

	public final MutableNumeric getNumeric() throws Exception {
		return getResult().getNumeric();
	}

	public final Object getObject() throws Exception {
		return getResult().getObject();
	}

	public final String getString() throws Exception {
		return getResult().getString();
	}

	public final byte[] getBytes() throws Exception {
		return getResult().getBytes();
	}

	public final int getDataType() {
		if (dataType < 0) {
			dataType = elseResult.getDataType();
			for (int i = 0; i < results.size(); i++) {
				dataType = ExpressionArithmetic.getDataType(dataType, results
						.get(i).getDataType());
			}
		}
		return dataType;
	}

	public final int getPrecision() {
		int precision = 0;
		for (int i = results.size() - 1; i >= 0; i--) {
			precision = Math.max(precision, results.get(i).getPrecision());
		}
		return precision;
	}

	public final int getScale() {
		int precision = 0;
		for (int i = results.size() - 1; i >= 0; i--) {
			precision = Math.max(precision, results.get(i).getScale());
		}
		return precision;
	}

	// ================================
	// private helper functions
	// ================================

	final private Expression getResult() throws Exception {
		for (int i = 0; i < cases.size(); i++) {
			if (cases.get(i).getBoolean())
				return results.get(i);
		}
		return elseResult;
	}

}
