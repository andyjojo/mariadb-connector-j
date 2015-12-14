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

import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.read.ReadResultPacketFactory;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MariaUpdateResult extends AbstractResult {
    private Protocol protocol;
    private long totalAffectedRows;
    private List<Long> insertIds;
    private List<Long> updateRows;

    /**
     * Create Streaming resultset.
     * @param packet first update packet info
     * @param protocol protocol
     */
    public MariaUpdateResult(Statement statement, OkPacket packet, Protocol protocol) {
        super(statement);
        this.protocol = protocol;
        this.totalAffectedRows = packet.getAffectedRows();
        this.insertIds = new ArrayList<>();
        this.insertIds.add(new Long(packet.getInsertId()));
        this.updateRows = new ArrayList<>();
        this.updateRows.add(this.totalAffectedRows);
    }

    public ResultType getResultType() {
        return ResultType.MODIFY;
    }

    /**
     * Create streaming resultset.
     * @param packet         the result set stream from the server
     * @param packetFetcher  packetfetcher
     * @param moreResult     are there more result
     * @return a MariaUpdateResult
     * @throws IOException    when something goes wrong while reading/writing from the server
     * @throws QueryException if there is an actual active result on the current connection
     */
    public static MariaUpdateResult createResult(Statement statement, OkPacket packet, ReadPacketFetcher packetFetcher, Protocol protocol, boolean moreResult)
            throws IOException, QueryException {

        if (moreResult && protocol.getActiveResult() != null) {
            throw new QueryException("There is an active result set on the current connection, "
                    + "which must be closed prior to opening a new one");
        }

        MariaUpdateResult mariaUpdateResultSet = new MariaUpdateResult(statement, packet, protocol);
        mariaUpdateResultSet.fetchAllResults(packetFetcher, moreResult);
        return mariaUpdateResultSet;
    }

    public void fetchAllResults(ReadPacketFetcher packetFetcher, boolean moreResult) throws IOException, QueryException {
        while (moreResult) {
            AbstractResultPacket resultPacket = ReadResultPacketFactory.createResultPacket(packetFetcher);
            switch (resultPacket.getResultType()) {
                case ERROR:
                    protocol.setActiveResult(null);
                    ErrorPacket errorPacket = (ErrorPacket) resultPacket;
                    throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());

                case EOF: //is EOF stream
                    final EndOfFilePacket endOfFilePacket = (EndOfFilePacket) resultPacket;
                    if (protocol.getActiveResult() == this) {
                        protocol.setActiveResult(null);
                    }
                    protocol.setHasWarnings(endOfFilePacket.getWarningCount() > 0);
                    if ((endOfFilePacket.getStatusFlags() & ServerStatus.MORE_RESULTS_EXISTS) != 0) {
                        //has more result
                        this.moreResult = protocol.fetchResult(statement, null, 0, false, ResultSet.TYPE_FORWARD_ONLY);
                    }
                    packetFetcher = null;
                    protocol = null;
                    break;

                case OK:
                    final OkPacket okpacket = (OkPacket) resultPacket;
                    moreResult = ((okpacket.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) != 0);
                    this.totalAffectedRows += okpacket.getAffectedRows();
                    this.insertIds.add(okpacket.getInsertId());
                    this.updateRows.add(okpacket.getAffectedRows());
                    break;

                case RESULTSET:
                    ResultSetPacket resultSetPacket = (ResultSetPacket) resultPacket;
                    this.moreResult = MariaSelectResultSet.createResult(statement, resultSetPacket, packetFetcher, protocol, false, 0, ResultSet.TYPE_FORWARD_ONLY);
                    packetFetcher = null;
                    protocol = null;
                    moreResult = false;
                    break;

                default:
                    throw new QueryException("Could not parse result", (short) -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState());
            }
        }
    }


    /**
     * When using rewrite statement, there can be many insert/update command send to database, according to max_allowed_packet size.
     * the result will be aggregate with this method to give only one result stream to client.
     * @param other other AbstractResult.
     */
    public void addResult(AbstractResult other) {
        if (other.prepareResult != null) {
            prepareResult = other.prepareResult;
        }
        isClosed = other.isClosed();
        if (other instanceof MariaUpdateResult) {
            MariaUpdateResult updateResult = (MariaUpdateResult) other;
            this.totalAffectedRows += updateResult.getTotalAffectedRows();
            this.insertIds.addAll(updateResult.getInsertIds());
            this.updateRows.addAll(updateResult.getUpdateRows());
        }
    }

    public List<Long> getInsertIds() {
        return insertIds;
    }

    public void setInsertIds(List<Long> insertIds) {
        this.insertIds = insertIds;
    }

    public long getTotalAffectedRows() {
        return totalAffectedRows;
    }

    public void setTotalAffectedRows(long totalAffectedRows) {
        this.totalAffectedRows = totalAffectedRows;
    }

    public List<Long> getUpdateRows() {
        return updateRows;
    }

    public void setUpdateRows(List<Long> updateRows) {
        this.updateRows = updateRows;
    }
}
