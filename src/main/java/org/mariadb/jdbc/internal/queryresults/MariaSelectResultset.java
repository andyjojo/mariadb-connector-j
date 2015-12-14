/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson

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

package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.packet.read.RawPacket;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.read.ReadResultPacketFactory;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.protocol.MasterProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.value.GeneratedKeyValueObject;
import org.mariadb.jdbc.internal.queryresults.value.MariaDbValueObject;
import org.mariadb.jdbc.internal.queryresults.value.ValueObject;
import org.mariadb.jdbc.internal.util.ExceptionCode;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.buffer.ReadUtil;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MariaSelectResultSet extends AbstractSelectResultSet {
    public static final MariaSelectResultSet EMPTY = createEmptyResultSet();

    private ReadPacketFetcher packetFetcher;
    private boolean isEof;
    private boolean binaryProtocol;
    private int dataFetchTime;
    private RowPacket rowPacket;
    private boolean streaming;
    protected int columnInformationLength;
    protected List<ValueObject[]> resultSet;
    protected int fetchSize;
    protected int resultSetScrollType;
    protected ResultType resultType;

    /**
     * Create Streaming resultset.
     * @param columnInformation column information
     * @param statement statement
     * @param fetcher stream fetcher
     * @param binaryProtocol is binary protocol ?
     * @param fetchSize      fetchSize
     * @param resultSetScrollType  one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultType on of the following <code>ResultType.SELECT</code>, <code>ResultType.MODIFY</code>, <code>ResultType.GENERATED</code>
     */
    public MariaSelectResultSet(ColumnInformation[] columnInformation, Statement statement, Protocol protocol, ReadPacketFetcher fetcher, boolean binaryProtocol, int fetchSize,
                                int resultSetScrollType, ResultType resultType) {
        super(columnInformation, protocol, statement);
        this.columnInformationLength = columnInformation.length;
        this.packetFetcher = fetcher;
        this.isEof = false;
        this.binaryProtocol = binaryProtocol;
        this.fetchSize = fetchSize;
        this.resultSetScrollType = resultSetScrollType;
        this.resultSet = new ArrayList<>();
        this.dataFetchTime = 0;
        this.rowPointer = -1;
        this.resultType = resultType;
    }

    /**
     * Create filled resultset.
     * @param columnInformation column information
     * @param protocol protocol
     * @param resultSetScrollType  one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultType on of the following <code>ResultType.SELECT</code>, <code>ResultType.MODIFY</code>, <code>ResultType.GENERATED</code>
     */
    public MariaSelectResultSet(ColumnInformation[] columnInformation, List<ValueObject[]> resultSet, Protocol protocol,
                                int resultSetScrollType, ResultType resultType) {
        super(columnInformation, protocol, null);
        this.columnInformationLength = columnInformation.length;
        this.isEof = false;
        this.binaryProtocol = false;
        this.fetchSize = 1;
        this.resultSetScrollType = resultSetScrollType;
        this.resultSet = resultSet;
        this.dataFetchTime = 0;
        this.rowPointer = -1;
        this.resultType = resultType;
    }

    /**
     * Create a result set from given data. Useful for creating "fake" resultsets for DatabaseMetaData, (one example is
     * MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param columnNames - string array of column names
     * @param columnTypes - column types
     * @param data - each element of this array represents a complete row in the ResultSet. Each value is given in its string representation, as in
     * MySQL text protocol, except boolean (BIT(1)) values that are represented as "1" or "0" strings
     * @param protocol protocol
     * @param findColumnReturnsOne - special parameter, used only in generated key result sets
     */
    public static ResultSet createResultSet(String[] columnNames, MariaDbType[] columnTypes, List<Long> data,
                                     Protocol protocol, boolean findColumnReturnsOne) {
        int columnNameLength = columnNames.length;
        ColumnInformation[] columns = new ColumnInformation[columnNameLength];

        for (int i = 0; i < columnNameLength; i++) {
            columns[i] = ColumnInformation.create(columnNames[i], columnTypes[i]);
        }

        List<ValueObject[]> rows = new ArrayList<>();
        for (Long rowData : data) {
            ValueObject[] row = new ValueObject[1];
            row[0] = new GeneratedKeyValueObject(rowData);
            rows.add(row);
        }
        if (findColumnReturnsOne) {
            return new MariaSelectResultSet(columns, rows, protocol, ResultSet.TYPE_SCROLL_SENSITIVE, ResultType.SELECT) {
                @Override
                public int findColumn(String name) {
                    return 1;
                }
            };
        }
        return new MariaSelectResultSet(columns, rows, protocol, ResultSet.TYPE_SCROLL_SENSITIVE, ResultType.SELECT);
    }


    /**
     * Create a result set from given data. Useful for creating "fake" resultsets for DatabaseMetaData, (one example is
     * MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param columnNames - string array of column names
     * @param columnTypes - column types
     * @param data - each element of this array represents a complete row in the ResultSet. Each value is given in its string representation, as in
     * MySQL text protocol, except boolean (BIT(1)) values that are represented as "1" or "0" strings
     * @param protocol protocol
     */
    public static ResultSet createResultSet(String[] columnNames, MariaDbType[] columnTypes, String[][] data,
                                     Protocol protocol) {
        int columnNameLength = columnNames.length;
        ColumnInformation[] columns = new ColumnInformation[columnNameLength];

        for (int i = 0; i < columnNameLength; i++) {
            columns[i] = ColumnInformation.create(columnNames[i], columnTypes[i]);
        }

        final byte[] boolTrue = {1};
        final byte[] boolFalse = {0};
        List<ValueObject[]> rows = new ArrayList<>();
        for (String[] rowData : data) {
            ValueObject[] row = new ValueObject[columnNameLength];

            if (rowData.length != columnNameLength) {
                throw new RuntimeException("Number of elements in the row != number of columns :" + rowData.length + " vs " + columnNameLength);
            }
            for (int i = 0; i < columnNameLength; i++) {
                byte[] bytes;
                if (rowData[i] == null) {
                    bytes = null;
                } else if (columnTypes[i] == MariaDbType.BIT) {
                    bytes = rowData[i].equals("0") ? boolFalse : boolTrue;
                } else {
                    try {
                        bytes = rowData[i].getBytes("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        //never append, UTF-8 is known
                        bytes = new byte[0];
                    }
                }
                row[i] = new MariaDbValueObject(bytes, columns[i], protocol.getOptions());
            }
            rows.add(row);
        }

        return new MariaSelectResultSet(columns, rows, protocol, ResultSet.TYPE_SCROLL_SENSITIVE, ResultType.SELECT);
    }


    /**
     * Create streaming resultset.
     * @param packet         the result set stream from the server
     * @param packetFetcher  packetfetcher
     * @param binaryProtocol is the mysql protocol binary
     * @param fetchSize      fetchSize
     * @param resultSetScrollType  one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return a StreamingQueryResult
     * @throws IOException    when something goes wrong while reading/writing from the server
     * @throws QueryException if there is an actual active result on the current connection
     */
    public static MariaSelectResultSet createResult(Statement statement, ResultSetPacket packet, ReadPacketFetcher packetFetcher, Protocol protocol, boolean binaryProtocol,
                                                    int fetchSize, int resultSetScrollType) throws IOException, QueryException {

        if (protocol.getActiveResult() != null) {
            throw new QueryException("There is an active result set on the current connection, "
                    + "which must be closed prior to opening a new one");
        }

        long fieldCount = packet.getFieldCount();
        ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            ci[i] = new ColumnInformation(rawPacket.getByteBuffer());
        }

        ByteBuffer bufferEof = packetFetcher.getReusableBuffer();
        if (!ReadUtil.eofIsNext(bufferEof)) {
            throw new QueryException("Packets out of order when reading field packets, expected was EOF stream. "
                    + "Packet contents (hex) = " + MasterProtocol.hexdump(bufferEof, 0));
        }
        MariaSelectResultSet mariaSelectResultset = new MariaSelectResultSet(ci, statement, protocol, packetFetcher, binaryProtocol, fetchSize, resultSetScrollType, ResultType.SELECT);
        mariaSelectResultset.initFetch();
        return mariaSelectResultset;
    }

    public void addResult(AbstractResult resultset) {
        if (resultset.getFailureObject() != null) {
            prepareResult = resultset.getFailureObject();
        }
        isClosed = resultset.isClosed();
    }

    private static MariaSelectResultSet createEmptyResultSet() {
        return new MariaSelectResultSet(new ColumnInformation[0], new ArrayList<ValueObject[]>(), null, ResultSet.TYPE_SCROLL_SENSITIVE, ResultType.SELECT);
    }


    public void initFetch() throws IOException, QueryException {
        if (binaryProtocol) {
            rowPacket = new BinaryRowPacket(columnsInformation, protocol.getOptions(), columnInformationLength);
        } else {
            rowPacket = new TextRowPacket(columnsInformation, protocol.getOptions(), columnInformationLength);
        }
        if (fetchSize == 0 || resultSetScrollType != ResultSet.TYPE_FORWARD_ONLY) {
            fetchAllResults();
            streaming = false;
        } else {
            protocol.setActiveResult(this);
            streaming = true;
        }
    }

    public boolean isBinaryProtocol() {
        return binaryProtocol;
    }

    private void fetchAllResults() throws IOException, QueryException {
        final List<ValueObject[]> valueObjects = new ArrayList<>();
        while (readNextValue(valueObjects)) {
            //fetch all results
        }
        dataFetchTime++;
        resultSet = valueObjects;
    }


    private void nextStreamingValue() throws IOException, QueryException {
        final List<ValueObject[]> valueObjects = new ArrayList<>();
        //fetch maximum fetchSize results
        int fetchSizeTmp = fetchSize;
        while (fetchSizeTmp > 0 && readNextValue(valueObjects)) {
            fetchSizeTmp--;
        }
        dataFetchTime++;
        resultSet = valueObjects;
    }

    public boolean readNextValue(List<ValueObject[]> values) throws IOException, QueryException {
        ByteBuffer buffer = packetFetcher.getReusableBuffer();
        byte initialByte = buffer.get(0);

        //is error Packet
        if (initialByte == (byte) 0xff) {
            protocol.setActiveResult(null);
            ErrorPacket errorPacket = (ErrorPacket) ReadResultPacketFactory.createResultPacket(buffer);
            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        //is EOF stream
        if ((initialByte == (byte) 0xfe && buffer.limit() < 9)) {
            final EndOfFilePacket endOfFilePacket = (EndOfFilePacket) ReadResultPacketFactory.createResultPacket(buffer);
            if (protocol.getActiveResult() == this) {
                protocol.setActiveResult(null);
            }
            protocol.setHasWarnings(endOfFilePacket.getWarningCount() > 0);
            if ((endOfFilePacket.getStatusFlags() & ServerStatus.MORE_RESULTS_EXISTS) != 0) {
                //has more result
                moreResult = protocol.fetchResult(statement, null, fetchSize, binaryProtocol, resultSetScrollType);
            }
            protocol = null;
            packetFetcher = null;
            isEof = true;
            return false;
        }
        values.add(rowPacket.getRow(packetFetcher, buffer));
        return true;
    }


    /**
     * Close resultset.
     */
    public void close() throws SQLException {
        super.close();
        if (protocol != null && protocol.getActiveResult() == this) {
            ReentrantLock lock = protocol.getLock();
            lock.lock();
            try {
                if (!isEof) {
                    for (; ; ) {
                        try {
                            if (protocol.getActiveResult() == null) {
                                return;
                            }
                            if (!next()) {
                                return;
                            }
                        } catch (SQLException qe) {
                            return;
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }


    @Override
    public boolean next() throws SQLException {
        if (rowPointer < resultSet.size() - 1) {
            rowPointer++;
            return true;
        } else {
            if (streaming) {
                if (isEof) {
                    return isEof;
                } else {
                    try {
                        nextStreamingValue();
                    } catch (IOException ioe) {
                        throw new SQLException(ioe);
                    } catch (QueryException queryException) {
                        throw new SQLException(queryException);
                    }
                    rowPointer = 0;
                    return resultSet.size() > 0;
                }
            } else {
                rowPointer = resultSet.size();
                return false;
            }
        }
    }

    @Override
    public ValueObject getValueObject(int position) throws SQLException {
        if (this.rowPointer < 0) {
            throwError("Current position is before the first row", ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        if (this.rowPointer >= resultSet.size()) {
            throwError("Current position is after the last row", ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        ValueObject[] row = resultSet.get(this.rowPointer);
        if (position <= 0 || position > row.length) {
            throwError("No such column: " + position, ExceptionCode.INVALID_PARAMETER_VALUE);
        }
        ValueObject vo = row[position - 1];
        this.lastGetWasNull = vo.isNull();
        return vo;
    }

    private void throwError(String message, ExceptionCode exceptionCode) throws SQLException {
        if (statement != null) {
            ExceptionMapper.throwException(new QueryException("Current position is before the first row", ExceptionCode.INVALID_PARAMETER_VALUE),
                    (MariaDbConnection) this.statement.getConnection(), this.statement);
        } else {
            throw new SQLException(message, exceptionCode.sqlState);
        }
    }

    public ResultType getResultType() {
        return resultType;
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (this.statement == null) {
            return null;
        }
        return this.statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (this.statement != null) {
            this.statement.clearWarnings();
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rowPointer == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (dataFetchTime > 0) {
            return rowPointer >= resultSet.size() && resultSet.size() > 0;
        }
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return dataFetchTime == 1 && rowPointer == 0 && resultSet.size() > 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (dataFetchTime > 0 && isEof) {
            return rowPointer == resultSet.size() - 1 && resultSet.size() > 0;
        } else if (streaming) {
            try {
                nextStreamingValue();
            } catch (IOException ioe) {
                throw new SQLException(ioe);
            } catch (QueryException queryException) {
                throw new SQLException(queryException);
            }
            return rowPointer == resultSet.size() - 1 && resultSet.size() > 0;
        }
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (streaming && resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = -1;
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (streaming && resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = resultSet.size();
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (streaming && resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = 0;
            return true;
        }
    }

    @Override
    public boolean last() throws SQLException {
        if (streaming && resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            rowPointer = resultSet.size() - 1;
            return true;
        }
    }

    @Override
    public int getRow() throws SQLException {
        if (streaming) {
            return 0;
        }
        return rowPointer + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (streaming && resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            if (row >= 0 && row <= resultSet.size()) {
                rowPointer = row - 1;
                return true;
            } else if (row < 0) {
                rowPointer = resultSet.size() + row;
            }
            return true;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (streaming && resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            int newPos = rowPointer + rows;
            if (newPos > -1 && newPos <= resultSet.size()) {
                rowPointer = newPos;
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        if (streaming && resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
        } else {
            if (rowPointer > -1) {
                rowPointer--;
                if (rowPointer == -1) {
                    return false;
                } else {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction == ResultSet.FETCH_REVERSE) {
            throw new SQLException("Invalid operation. Allowed direction are ResultSet.FETCH_FORWARD and ResultSet.FETCH_UNKNOWN");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        if (streaming && this.fetchSize == 0) {
            try {
                while (readNextValue(resultSet)) {
                    //fetch all results
                }
            } catch (IOException ioException) {
                throw new SQLException(ioException);
            } catch (QueryException queryException) {
                throw new SQLException(queryException);
            }

            dataFetchTime++;
            streaming = false;

        }
        this.fetchSize = fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return resultSetScrollType;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

}
