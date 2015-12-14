package org.mariadb.jdbc;
/*
MariaDB Client for Java

Copyright (c) 2015 MariaDB.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye, Stephane Giron

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.AbstractResult;
import org.mariadb.jdbc.internal.queryresults.MariaUpdateResult;
import org.mariadb.jdbc.internal.queryresults.ResultType;
import org.mariadb.jdbc.internal.util.ExceptionCode;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class MariaDbServerPreparedStatement extends AbstractMariaDbPrepareStatement {
    protected boolean binaryData = true;
    String sql;
    PrepareResult prepareResult;
    boolean returnTableAlias = false;
    int parameterCount;
    MariaDbResultSetMetaData metadata;
    MariaDbParameterMetaData parameterMetaData;
    ParameterHolder[] currentParameterHolder;
    List<ParameterHolder[]> queryParameters = new ArrayList<>();

    /**
     * Constructor for creating Server prepared statement.
     * @param connection current connection
     * @param sql Sql String to prepare
     * @param autoGeneratedKeys must generated keys be retrieved when batch insert ?
     * @param resultSetType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws SQLException exception
     */
    public MariaDbServerPreparedStatement(MariaDbConnection connection, String sql, final int autoGeneratedKeys, int resultSetType) throws SQLException {
        super(connection, autoGeneratedKeys, resultSetType);
        useFractionalSeconds = connection.getProtocol().getOptions().useFractionalSeconds;
        this.sql = sql;
        prepare(sql);
    }

    private void prepare(String sql) throws SQLException {
        checkClose();
        try {
            connection.lock.lock();
            try {
                if (protocol.hasUnreadData()) {
                    ExceptionMapper.throwException(new QueryException("There is an open result set on the current connection, which must be "
                            + "closed prior to executing a query"), connection, this);
                }
                prepareResult = protocol.prepare(sql);
            } finally {
                connection.lock.unlock();
            }
            parameterCount = prepareResult.parameters.length;
            if (parameterCount > 0) {
                currentParameterHolder = new ParameterHolder[prepareResult.parameters.length];
            }
            returnTableAlias = protocol.getOptions().useOldAliasMetadataBehavior;
            metadata = new MariaDbResultSetMetaData(prepareResult.columns,
                    protocol.getDataTypeMappingFlags(), returnTableAlias);
            parameterMetaData = new MariaDbParameterMetaData(prepareResult.parameters);
        } catch (QueryException e) {
            try {
                this.close();
            } catch (Exception ee) {
                //eat exception.
            }
            ExceptionMapper.throwException(e, connection, this);
        }
    }

    @Override
    protected boolean isNoBackslashEscapes() {
        return connection.noBackslashEscapes;
    }

    @Override
    protected boolean useFractionalSeconds() {
        return useFractionalSeconds;
    }

    @Override
    protected Calendar cal() {
        return protocol.getCalendar();
    }

    protected void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {

        try {
            currentParameterHolder[parameterIndex - 1] = holder;
        } catch (ArrayIndexOutOfBoundsException a) {
            throw ExceptionMapper.getSqlException("Could not set parameter at position " + parameterIndex
                    + ", parameter length is " + parameterCount);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        //check
        for (int i = 0; i < parameterCount; i++) {
            if (currentParameterHolder[i] == null) {
                ExceptionMapper.throwException(new QueryException("Parameter at position " + (i + 1) + " is not set", -1, "07004"),
                        connection, this);
            }
        }
        queryParameters.add(currentParameterHolder.clone());
    }

    public void clearBatch() {
        queryParameters.clear();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return parameterMetaData;
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }


    /**
     * <p>Submits a batch of send to the database for execution and if all send execute successfully, returns an
     * array of update counts. The <code>int</code> elements of the array that is returned are ordered to correspond to
     * the send in the batch, which are ordered according to the order in which they were added to the batch. The
     * elements in the array returned by the method <code>executeBatch</code> may be one of the following:</p>
     * <ol><li>A number greater than or equal to zero -- indicates that the command was processed successfully and is an update
     * count giving the number of rows in the database that were affected by the command's execution
     * <li>A value of <code>SUCCESS_NO_INFO</code> -- indicates that the command was processed successfully but that the number of rows
     * affected is unknown.
     * If one of the send in a batch update fails to execute properly, this method throws a
     * <code>BatchUpdateException</code>, and a JDBC driver may or may not continue to process the remaining send in
     * the batch.  However, the driver's behavior must be consistent with a particular DBMS, either always continuing to
     * process send or never continuing to process send.  If the driver continues processing after a failure,
     * the array returned by the method <code>BatchUpdateException.getUpdateCounts</code> will contain as many elements
     * as there are send in the batch, and at least one of the elements will be the following:
     * <li>A value of <code>EXECUTE_FAILED</code> -- indicates that the command failed to execute successfully and
     * occurs only if a driver continues to process send after a command fails </ol>
     * <p>The possible implementations and return values have been modified in the Java 2 SDK, Standard Edition, version
     * 1.3 to accommodate the option of continuing to proccess send in a batch update after a
     * <code>BatchUpdateException</code> object has been thrown.</p>
     *
     * @return an array of update counts containing one element for each command in the batch.  The elements of the
     * array are ordered according to the order in which send were added to the batch.
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed
     *                               <code>Statement</code> or the driver does not support batch statements. Throws
     *                               {@link java.sql.BatchUpdateException} (a subclass of <code>SQLException</code>) if
     *                               one of the send sent to the database fails to execute properly or attempts to
     *                               return a result set.
     * @see #addBatch
     * @see java.sql.DatabaseMetaData#supportsBatchUpdates
     * @since 1.3
     */
    @Override
    public int[] executeBatch() throws SQLException {
        checkClose();
        if (parameterCount > 0 && queryParameters.size() == 0) {
            throw ExceptionMapper.getSqlException("No Parameters set. The command addBatch() must have been set");
        }
        int counter = 0;
        int[] ret = new int[queryParameters.size()];

        MariaDbType[] parameterTypeHeader = new MariaDbType[parameterCount];
        connection.lock.lock();
        executing = true;
        QueryException exception = null;
        try {
            executeQueryProlog();
            int queryParameterSize = queryParameters.size();
            queryResult = null;
            for (; counter < queryParameterSize; counter++) {
                try {
                    AbstractResult tmpQueryResult = protocol.executePreparedQuery(this, sql, queryParameters.get(counter), prepareResult, parameterTypeHeader,
                            fetchSize, resultSetType);

                    // in case of failover
                    if (tmpQueryResult.getFailureObject() != null) {
                        prepareResult = queryResult.getFailureObject();
                    }
                    if (queryResult == null) {
                        queryResult = tmpQueryResult;
                    } else {
                        queryResult.addResult(tmpQueryResult);
                    }
                } catch (QueryException e) {
                    exception = e;
                    if (!protocol.getOptions().continueBatchOnError) {
                        break;
                    }
                }
            }
            executeQueryEpilog(exception, sql);
            executing = false;

            MariaUpdateResult updateResult = (MariaUpdateResult) queryResult;
            int[] returnData = new int[updateResult.getUpdateRows().size()];
            for (int res = 0 ; res < returnData.length; res++) {
                returnData[res] = updateResult.getUpdateRows().get(res).intValue();
            }
            return returnData;

        } catch (SQLException sqle) {
            clearBatch();
            throw new BatchUpdateException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), Arrays.copyOf(ret, counter), sqle);
        } finally {
            connection.lock.unlock();
            clearBatch();
        }

    }


    private boolean executeInternal(ParameterHolder[] parameters, MariaDbType[] parameterTypeHeader) throws SQLException {
        executing = true;
        QueryException exception = null;
        executeQueryProlog();
        try {
            queryResult = protocol.executePreparedQuery(this, sql, parameters, prepareResult, parameterTypeHeader, fetchSize, resultSetType);

            // in case of failover
            if (queryResult.getFailureObject() != null) {
                prepareResult = queryResult.getFailureObject();
            }
            return (queryResult.getResultType() == ResultType.SELECT);
        } catch (QueryException e) {
            exception = e;
            return false;
        } finally {
            executeQueryEpilog(exception, sql);
            executing = false;
        }
    }


    /*
     Reset timeout after query, re-throw  SQL  exception
    */
    private void executeQueryEpilog(QueryException exception, String sql) throws SQLException {

        if (timerTask != null) {
            timerTask.cancel();
            timerTask = null;
        }

        if (isTimedout) {
            isTimedout = false;
            exception = new QueryException("Query timed out", 1317, "JZ0002", exception);
        }

        if (exception == null) {
            return;
        }

        /* Include query into exception message, if dumpQueriesOnException is true,
         * or on SQL syntax error (MySQL error code 1064).
         *
         * If SQL query is too long, truncate it to reasonable (for exception messages)
         * length.
         */
        if (protocol.getOptions().dumpQueriesOnException
                || exception.getErrorCode() == 1064) {
            String queryString = new String(sql);
            if (queryString.length() > 4096) {
                queryString = queryString.substring(0, 4096);
            }
            exception.setMessage(exception.getMessage() + "\nQuery is:\n" + queryString);
        }

        //if has a failover, closing the statement
        if (exception.getSqlState() != null && exception.getSqlState().startsWith("08")) {
            close();
        }

        ExceptionMapper.throwException(exception, connection, this);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        return getResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        execute();
        return getUpdateCount();
    }

    @Override
    public void clearParameters() throws SQLException {
        currentParameterHolder = new ParameterHolder[prepareResult.parameters.length];
    }

    @Override
    public boolean execute() throws SQLException {
        isClosed();
        for (int i = 0; i < parameterCount; i++) {
            if (currentParameterHolder[i] == null) {
                ExceptionMapper.throwException(new QueryException("Invalid parameter value", ExceptionCode.WRONG_NUMBER_OF_ARGUMENT), connection, this);
            }
        }
        boolean result = false;
        connection.lock.lock();
        try {
            result = executeInternal(currentParameterHolder, new MariaDbType[parameterCount]);
        } catch (SQLException sqle) {
            throw new BatchUpdateException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), new int[]{0}, sqle);
        } finally {
            connection.lock.unlock();
            clearBatch();
        }
        return result;
    }


    /**
     * <p>Releases this <code>Statement</code> object's database and JDBC resources immediately instead of waiting for this
     * to happen when it is automatically closed. It is generally good practice to release resources as soon as you are
     * finished with them to avoid tying up database resources.</p>
     * <p>Calling the method <code>close</code> on a <code>Statement</code> object that is already closed has no effect.</p>
     * <p><B>Note:</B>When a <code>Statement</code> object is closed, its current <code>ResultSet</code> object, if one
     * exists, is also closed.</p>
     *
     * @throws java.sql.SQLException if a database access error occurs
     */
    @Override
    public void close() throws SQLException {
        if (queryResult != null) {
            queryResult.close();
            queryResult = null;
        }

        if (protocol.isConnected()) {
            try {
                protocol.releasePrepareStatement(sql, prepareResult.statementId);
            } catch (QueryException e) {
                //if (log.isDebugEnabled()) log.debug("Error releasing preparedStatement", e);
            }
        }

        super.close();
        isClosed = true;

        if (connection == null || connection.pooledConnection == null || connection.pooledConnection.statementEventListeners.isEmpty()) {
            return;
        }
        connection.pooledConnection.fireStatementClosed(this);
        connection = null;
        protocol = null;
    }

    /**
     * Return sql String value.
     * @return String representation
     */
    public String toString() {
        StringBuffer sb = new StringBuffer("sql : '" + sql + "'");
        if (parameterCount > 0) {
            sb.append(", parameters : [");
            for (int i = 0; i < parameterCount; i++) {
                if (currentParameterHolder[i] == null) {
                    sb.append("null");
                } else {
                    sb.append(currentParameterHolder[i].toString());
                }
                if (i != parameterCount - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
