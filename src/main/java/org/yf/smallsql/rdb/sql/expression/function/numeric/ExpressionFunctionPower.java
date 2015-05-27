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
 * ExpressionFunctionPower.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package org.yf.smallsql.rdb.sql.expression.function.numeric;

import org.yf.smallsql.rdb.sql.expression.function.ExpressionFunctionReturnFloat;
import org.yf.smallsql.rdb.sql.parser.SQLTokenizer;

public final class ExpressionFunctionPower extends ExpressionFunctionReturnFloat {

	public final int getFunction() {
		return SQLTokenizer.POWER;
	}

	public boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull();
	}

	public final double getDouble() throws Exception {
		if (isNull())
			return 0;
		return Math.pow(param1.getDouble(), param2.getDouble());
	}
}