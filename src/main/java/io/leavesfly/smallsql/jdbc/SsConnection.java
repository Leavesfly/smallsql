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
 * SSConnection.java
 * ---------------
 * Author: Volker Berlin
 *
 */
package io.leavesfly.smallsql.jdbc;

import java.nio.channels.FileChannel;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import io.leavesfly.smallsql.jdbc.metadata.SsDatabaseMetaData;
import io.leavesfly.smallsql.jdbc.statement.SsCallableStatement;
import io.leavesfly.smallsql.jdbc.statement.SsStatement;
import io.leavesfly.smallsql.jdbc.statement.SsPreparedStatement;
import io.leavesfly.smallsql.jdbc.statement.SsSavepoint;
import io.leavesfly.smallsql.lang.Language;
import io.leavesfly.smallsql.logger.Logger;
import io.leavesfly.smallsql.rdb.engine.Database;
import io.leavesfly.smallsql.rdb.engine.TransactionStep;

public class SsConnection implements Connection {

    private boolean readOnly;
    private Database database;
    private boolean autoCommit = true;

    // see also getDefaultTransactionIsolation
    public int isolationLevel = TRANSACTION_READ_COMMITTED;
    private List<TransactionStep> commitPages = new ArrayList<TransactionStep>();
    /**
     * The time on which a transaction is starting.
     */
    private long transactionTime;
    private final SsDatabaseMetaData metadata;
    private int holdability;
    public final Logger log;

    public SsConnection(Properties props) throws SQLException {
        SmallSQLException.setLanguage(props.get("locale"));
        log = new Logger();
        String name = props.getProperty("dbpath");
        readOnly = "true".equals(props.getProperty("readonly"));
        boolean create = "true".equals(props.getProperty("create"));
        database = Database.getDatabase(name, this, create);
        metadata = new SsDatabaseMetaData(this);
    }

    /**
     * Create a copy of the Connection with it own transaction room.
     *
     * @param con the original Connection
     */
    public SsConnection(SsConnection con) {
        readOnly = con.readOnly;
        database = con.database;
        metadata = con.metadata;
        log = con.log;
    }

    /**
     * @param returnNull If null is a valid return value for the case of not connected
     *                   to a database.
     * @throws SQLException If not connected and returnNull is false.
     */
    public Database getDatabase(boolean returnNull) throws SQLException {
        testClosedConnection();
        if (!returnNull && database == null)
            throw SmallSQLException.create(Language.DB_NOTCONNECTED);
        return database;
    }

    /**
     * Get a monitor object for all synchronized blocks on connection base.
     * Multiple calls return the same object.
     *
     * @return a unique object of this connection
     */
    public Object getMonitor() {
        return this;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new SsStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new SsPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new SsCallableStatement(this, sql);
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (log.isLogging())
            log.println("AutoCommit:" + autoCommit);
        if (this.autoCommit != autoCommit) {
            commit();
            this.autoCommit = autoCommit;
        }
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * Add a page for later commit or rollback.
     */
    public void add(TransactionStep storePage) throws SQLException {
        testClosedConnection();
        synchronized (getMonitor()) {
            commitPages.add(storePage);
        }
    }

    @Override
    public void commit() throws SQLException {
        log.println("Commit");
        testClosedConnection();
        synchronized (getMonitor()) {
            try {
                int count = commitPages.size();
                for (int i = 0; i < count; i++) {
                    TransactionStep page = (TransactionStep) commitPages.get(i);
                    page.commit();
                }
                for (int i = 0; i < count; i++) {
                    TransactionStep page = (TransactionStep) commitPages.get(i);
                    page.freeLock();
                }
                commitPages.clear();
                transactionTime = System.currentTimeMillis();
            } catch (Throwable e) {
                rollback();
                throw SmallSQLException.createFromException(e);
            }
        }
    }

    /**
     * Discard all changes of a file because it was deleted.
     */
    public void rollbackFile(FileChannel raFile) throws SQLException {
        testClosedConnection();
        // remove the all commits that point to this table
        synchronized (getMonitor()) {
            for (int i = commitPages.size() - 1; i >= 0; i--) {
                TransactionStep page = (TransactionStep) commitPages.get(i);
                if (page.raFile == raFile) {
                    page.rollback();
                    page.freeLock();
                }
            }
        }
    }

    public void rollback(int savepoint) throws SQLException {
        testClosedConnection();
        synchronized (getMonitor()) {
            for (int i = commitPages.size() - 1; i >= savepoint; i--) {
                TransactionStep page = (TransactionStep) commitPages.remove(i);
                page.rollback();
                page.freeLock();
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        log.println("Rollback");
        testClosedConnection();
        synchronized (getMonitor()) {
            int count = commitPages.size();
            for (int i = 0; i < count; i++) {
                TransactionStep page = (TransactionStep) commitPages.get(i);
                page.rollback();
                page.freeLock();
            }
            commitPages.clear();
            transactionTime = System.currentTimeMillis();
        }
    }

    @Override
    public void close() throws SQLException {
        rollback();
        database = null;
        commitPages = null;
        Database.closeConnection(this);
    }

    /**
     * Test if the connection was closed. for example from another thread.
     *
     * @throws SQLException if the connection was closed.
     */
    public final void testClosedConnection() throws SQLException {
        if (isClosed())
            throw SmallSQLException.create(Language.CONNECTION_CLOSED);
    }

    @Override
    public boolean isClosed() {
        return (commitPages == null);
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return metadata;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        // TODO Connection ReadOnly implementing
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        testClosedConnection();
        database = Database.getDatabase(catalog, this, false);
    }

    @Override
    public String getCatalog() {
        if (database == null)
            return "";
        return database.getName();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (!metadata.supportsTransactionIsolationLevel(level)) {
            throw SmallSQLException.create(Language.ISOLATION_UNKNOWN, String.valueOf(level));
        }
        isolationLevel = level;
    }

    @Override
    public int getTransactionIsolation() {
        return isolationLevel;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // TODO support for Warnings
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SsStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new SsPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SsCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    public Map getTypeMap() {
        return null;
    }

    public void setTypeMap(Map map) {
        // TODO support for TypeMap
    }

    @Override
    public void setHoldability(int holdability) {
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() {
        return holdability;
    }

    public int getSavepoint() throws SQLException {
        testClosedConnection();
        return commitPages.size(); // the call is atomic, that it need not be
        // synchronized
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return new SsSavepoint(getSavepoint(), null, transactionTime);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return new SsSavepoint(getSavepoint(), name, transactionTime);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (savepoint instanceof SsSavepoint) {
            if (((SsSavepoint) savepoint).transactionTime != transactionTime) {
                throw SmallSQLException.create(Language.SAVEPT_INVALID_TRANS);
            }
            rollback(savepoint.getSavepointId());
            return;
        }
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (savepoint instanceof SsSavepoint) {
            ((SsSavepoint) savepoint).transactionTime = 0;
            return;
        }
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, new Object[]{savepoint});
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        // TODO resultSetHoldability
        return new SsStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        // TODO resultSetHoldability
        return new SsPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        // TODO resultSetHoldability
        return new SsCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        SsPreparedStatement pr = new SsPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(autoGeneratedKeys);
        return pr;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        SsPreparedStatement pr = new SsPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(columnIndexes);
        return pr;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        SsPreparedStatement pr = new SsPreparedStatement(this, sql);
        pr.setNeedGeneratedKeys(columnNames);
        return pr;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }


    public void setSchema(String schema) throws SQLException {
        // TODO Auto-generated method stub

    }


    public String getSchema() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }


    public void abort(Executor executor) throws SQLException {
        // TODO Auto-generated method stub

    }


    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // TODO Auto-generated method stub

    }


    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
}