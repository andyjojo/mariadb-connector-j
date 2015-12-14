package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import javax.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class PooledConnectionTest extends BaseTest {
    @Test
    public void testPooledConnectionClosed() throws Exception {
        ConnectionPoolDataSource ds = new MariaDbDataSource(hostname, port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        Connection connection = pc.getConnection();
        MyEventListener listener = new MyEventListener();
        pc.addConnectionEventListener(listener);
        pc.addStatementEventListener(listener);
        connection.close();
        Assert.assertTrue(listener.closed);
       /* Verify physical connection is still ok */
        connection.createStatement().execute("select 1");

       /* close physical connection */
        pc.close();
       /* Now verify physical connection is gone */
        try {
            connection.createStatement().execute("select 1");
            Assert.assertFalse("should never get there", true);
        } catch (Exception e) {
            //eat exception
        }
    }

    @Test
    public void testPooledConnectionException() throws Exception {
        ConnectionPoolDataSource ds = new MariaDbDataSource(hostname, port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        MyEventListener listener = new MyEventListener();
        pc.addConnectionEventListener(listener);
        MariaDbConnection connection = (MariaDbConnection) pc.getConnection();

       /* Ask server to abort the connection */
        try {
            connection.createStatement().execute("KILL CONNECTION_ID()");
        } catch (Exception e) {
         /* exception is expected here, server sends query aborted */
        }

       /* Try to read  after server side closed the connection */
        try {
            connection.createStatement().execute("SELECT 1");
            Assert.assertTrue("should never get there", false);
        } catch (SQLException e) {
            //eat Exception
        }
        pc.close();
        //assertTrue(failover.closed);
    }


    @Test
    public void testPooledConnectionStatementError() throws Exception {
        ConnectionPoolDataSource ds = new MariaDbDataSource(hostname, port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        MyEventListener listener = new MyEventListener();
        pc.addStatementEventListener(listener);
        MariaDbConnection connection = (MariaDbConnection) pc.getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT ?");
        try {
            ps.execute();
            Assert.assertTrue("should never get there", false);
        } catch (Exception e) {
            Assert.assertTrue(listener.statementErrorOccured && listener.sqlException.getSQLState().equals("07001"));
        }
        ps.close();
        Assert.assertTrue(listener.statementClosed);
        pc.close();
    }
}
