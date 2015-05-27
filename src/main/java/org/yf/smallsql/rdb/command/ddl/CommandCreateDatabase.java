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
 * CommandCreateDatabase.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package org.yf.smallsql.rdb.command.ddl;

import java.io.*;

import org.yf.smallsql.jdbc.SmallSQLException;
import org.yf.smallsql.jdbc.SsConnection;
import org.yf.smallsql.jdbc.statement.SsStatement;
import org.yf.smallsql.lang.Language;
import org.yf.smallsql.logger.Logger;
import org.yf.smallsql.rdb.command.Command;
import org.yf.smallsql.rdb.sql.parser.SQLTokenizer;
import org.yf.smallsql.util.Utils;

public class CommandCreateDatabase extends Command {

	public CommandCreateDatabase(Logger log, String name) {
		super(log);
		this.type = SQLTokenizer.DATABASE;
		if (name.startsWith("file:"))
			name = name.substring(5);
		this.name = name;
	}

	@Override
	public void executeImpl(SsConnection con, SsStatement st) throws Exception {
		if (con.isReadOnly()) {
			throw SmallSQLException.create(Language.DB_READONLY);
		}
		File dir = new File(name);
		dir.mkdirs();
		if (!new File(dir, Utils.MASTER_FILENAME).createNewFile()) {
			throw SmallSQLException.create(Language.DB_EXISTENT, name);
		}
	}

}