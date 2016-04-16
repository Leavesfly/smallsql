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
 * ExpressionFunctionRepeat.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 24.06.2006
 */
package io.leavesfly.smallsql.rdb.sql.expression.function.string;

import java.io.ByteArrayOutputStream;

import io.leavesfly.smallsql.jdbc.metadata.SsResultSetMetaData;
import io.leavesfly.smallsql.rdb.sql.expression.function.ExpressionFunctionReturnP1StringAndBinary;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;

/**
 * @author Volker Berlin
 */
public class ExpressionFunctionRepeat extends ExpressionFunctionReturnP1StringAndBinary {

	public final int getFunction() {
		return SQLTokenizer.REPEAT;
	}

	public final byte[] getBytes() throws Exception {
		if (isNull())
			return null;
		byte[] bytes = param1.getBytes();
		int count = param2.getInt();
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		for (int i = 0; i < count; i++) {
			buffer.write(bytes);
		}
		return buffer.toByteArray();
	}

	public final String getString() throws Exception {
		if (isNull())
			return null;
		String str = param1.getString();
		int count = param2.getInt();
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < count; i++) {
			buffer.append(str);
		}
		return buffer.toString();
	}

	public int getPrecision() {
		return SsResultSetMetaData.getDataTypePrecision(getDataType(), -1);
	}

}
