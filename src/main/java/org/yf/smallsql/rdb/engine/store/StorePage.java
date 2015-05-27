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
 * StorePage.java
 * ---------------
 * Author: Volker Berlin
 * 
 * Created on 28.08.2004
 */
package org.yf.smallsql.rdb.engine.store;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;

import org.yf.smallsql.jdbc.SmallSQLException;
import org.yf.smallsql.rdb.engine.TransactionStep;

/**
 * @author Volker Berlin
 */
public class StorePage extends TransactionStep {
	protected byte[] page; // data of one page
	int pageSize;
	public long fileOffset; // position in file

	public StorePage(byte[] page, int pageSize, FileChannel raFile, long fileOffset) {
		super(raFile);
		this.page = page;
		this.pageSize = pageSize;
		this.fileOffset = fileOffset;
	}

	final void setPageData(byte[] data, int size) {
		page = data;
		pageSize = size;
	}

	@Override
	public long commit() throws SQLException {
		try {
			// rsFile == null --> rollback()
			// page == null --> only a write lock, no data
			if (raFile != null && page != null) {
				// if new page then append at end of file
				ByteBuffer buffer = ByteBuffer.wrap(page, 0, pageSize);
				synchronized (raFile) {
					if (fileOffset < 0) {
						fileOffset = raFile.size();
					}
					raFile.position(fileOffset);
					raFile.write(buffer);
				}
				// raFile.getFD().sync();
			}
			return fileOffset;
		} catch (Exception e) {
			throw SmallSQLException.createFromException(e);
		}
	}

	@Override
	public final void rollback() {
		raFile = null;
	}
}
