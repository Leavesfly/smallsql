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
 * CommandTable.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package io.leavesfly.smallsql.rdb.command.ddl;

import java.sql.SQLException;

import io.leavesfly.smallsql.rdb.engine.index.IndexDescription;
import io.leavesfly.smallsql.rdb.engine.store.TableStorePage;
import io.leavesfly.smallsql.rdb.engine.table.Column;
import io.leavesfly.smallsql.rdb.engine.table.Columns;
import io.leavesfly.smallsql.rdb.engine.table.ForeignKey;
import io.leavesfly.smallsql.rdb.sql.SQLParser;
import io.leavesfly.smallsql.jdbc.SmallSQLException;
import io.leavesfly.smallsql.jdbc.SsConnection;
import io.leavesfly.smallsql.jdbc.statement.SsStatement;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.logger.Logger;
import io.leavesfly.smallsql.rdb.command.Command;
import io.leavesfly.smallsql.rdb.engine.Database;
import io.leavesfly.smallsql.rdb.engine.Table;
import io.leavesfly.smallsql.rdb.engine.index.IndexDescriptions;
import io.leavesfly.smallsql.rdb.engine.table.ForeignKeys;
import io.leavesfly.smallsql.rdb.sql.parser.SQLTokenizer;


public final class CommandTable extends Command{

	final private Columns columns = new Columns();
	final private IndexDescriptions indexes = new IndexDescriptions();
    final private ForeignKeys foreignKeys = new ForeignKeys();
    final private int tableCommandType;
    
	
    public CommandTable( Logger log, String catalog, String name, int tableCommandType ){
    	super(log);
        this.type = SQLTokenizer.TABLE;
        this.catalog = catalog;
        this.name = name;
        this.tableCommandType = tableCommandType;
    }
    

    /**
     * Add a column definition. This is used from the SQLParser.
     * 
     * @throws SQLException
     *             if the column already exist in the list.
     * @see SQLParser#createTable
     */
    public void addColumn(Column column) throws SQLException{
        addColumn(columns, column);
    }
	
	
    public void addIndex( IndexDescription indexDescription ) throws SQLException{
		indexes.add(indexDescription);
	}
	
	
    public void addForeingnKey(ForeignKey key){
        foreignKeys.add(key);
    }
    
    
    public  void executeImpl(SsConnection con, SsStatement st) throws Exception{
        Database database = catalog == null ? 
                con.getDatabase(false) : 
                Database.getDatabase( catalog, con, false );
        switch(tableCommandType){
        case SQLTokenizer.CREATE:
            database.createTable( con, name, columns, indexes, foreignKeys );
            break;
        case SQLTokenizer.ADD:
            con = new SsConnection(con);
            //TODO disable the transaction to reduce memory use.
            Table oldTable = (Table)database.getTableView( con, name);
            
            // Request a TableLock and hold it for the completely ALTER TABLE command
            TableStorePage tableLock = oldTable.requestLock( con, SQLTokenizer.ALTER, -1);
            String newName = "#" + System.currentTimeMillis() + this.hashCode();
            try{
                Columns oldColumns = oldTable.columns;
                Columns newColumns = oldColumns.copy();
                for(int i = 0; i < columns.size(); i++){
                    addColumn(newColumns, columns.get(i));
                }
                
                Table newTable = database.createTable( con, newName, newColumns, oldTable.indexes, indexes, foreignKeys );
                StringBuffer buffer = new StringBuffer(256);
                buffer.append("INSERT INTO ").append( newName ).append( '(' );
                for(int c=0; c<oldColumns.size(); c++){
                    if(c != 0){
                        buffer.append( ',' );
                    }
                    buffer.append( oldColumns.get(c).getName() );
                }
                buffer.append( ")  SELECT * FROM " ).append( name );
                con.createStatement().execute( buffer.toString() );
                
                database.replaceTable( oldTable, newTable );
            }catch(Exception ex){
                //Remove all from the new table
                try {
                    database.dropTable(con, newName);
                } catch (Exception ex1) {/* ignore it */}
                try{
                    indexes.drop(database);
                } catch (Exception ex1) {/* ignore it */}
                throw ex;
            }finally{
                tableLock.freeLock();
            }
            break;
        default:
            throw new Error();
        }
    }


    private void addColumn(Columns cols, Column column) throws SQLException{
        if(cols.get(column.getName()) != null){
            throw SmallSQLException.create(Language.COL_DUPLICATE, column.getName());
        }
        cols.add(column);
    }
}