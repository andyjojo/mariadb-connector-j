package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ReplicationFailoverTest extends BaseReplication {

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxyReplicationUrl;
        Assume.assumeTrue(initialReplicationUrl != null);
    }

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        defaultUrl = initialReplicationUrl;
        currentType = HaMode.REPLICATION;
    }


    @Test
    public void readOnlyPropagatesToServerAlias() throws SQLException {
        assureReadOnly(true);
    }

    @Test
    public void assureReadOnly() throws SQLException {
        assureReadOnly(false);
    }

    private void assureReadOnly(boolean useAlias) throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection(useAlias?"&readOnlyPropagatesToServer=true":"&assureReadOnly=true", false);
            connection.setReadOnly(true);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists replicationDelete" + jobId);
            stmt.execute("create table replicationDelete" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");
            assertTrue(connection.isReadOnly());
            try {
                if (!isMariaDbServer(connection) || !requireMinimumVersion(connection, 5, 7)) {
                    //on version >= 5.7 use SESSION READ-ONLY, before no control
                    Assume.assumeTrue(false);
                }
                stmt.execute("drop table  if exists replicationDelete" + jobId);
                fail();
            } catch (SQLException e) {
                //normal exception
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void pingReconnectAfterFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=5&queriesBeforeRetryMaster=50000", true);
            Statement st = connection.createStatement();
            final int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            long stoppedTime = System.nanoTime();

            try {
                st.execute("SELECT 1");
            } catch (SQLException e) {
                //normal exception
            }

            connection.setReadOnly(true);
            st = connection.createStatement();
            restartProxy(masterServerId);
            try {
                connection.setReadOnly(false);
                fail();
            } catch (SQLException e) {
                //normal exception
            }


            boolean loop = true;
            while (loop) {
                try {
                    Thread.sleep(250);
                    int currentHost = getServerId(connection);
                    if (masterServerId == currentHost) {
                        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedTime);
                        assertTrue(duration > 5 * 1000);
                        loop = false;
                    }
                } catch (SQLException e) {
                    //eat exception
                }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedTime);
                if (duration > 20 * 1000) {
                    fail();
                }
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            assertFalse(slaveServerId == masterServerId);
            assertTrue(connection.isReadOnly());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test()
    public void masterWithoutFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);
            connection.setReadOnly(false);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            try {
                connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
                fail();
            } catch (SQLException e) {
                assertTrue(true);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void checkBackOnMasterOnSlaveFail() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=10&failOnReadOnly=true", true);
            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            try {
                st.execute("SELECT 1");
                assertTrue(connection.isReadOnly());
            } catch (SQLException e) {
                fail();
            }

            long stoppedTime = System.nanoTime();
            restartProxy(masterServerId);
            boolean loop = true;
            while (loop) {
                Thread.sleep(250);
                try {
                    if (!connection.isReadOnly()) {
                        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedTime);
                        assertTrue(duration > 10 * 1000);
                        loop = false;
                    }
                } catch (SQLException e) {
                    //eat exception
                }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedTime);
                if (duration > 30 * 1000) {
                    fail();
                }
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);
            Statement st = connection.createStatement();

            final int masterServerId = getServerId(connection);
            st.execute("drop table  if exists multinodeTransaction");
            st.execute("create table multinodeTransaction (id int not null primary key , amount int not null) "
                    + "ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
            stopProxy(masterServerId);
            assertTrue(inTransaction(connection));
            try {
                //with autoreconnect but in transaction, query must throw an error
                st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
                fail();
            } catch (SQLException e) {
                //normal exception
            }
            restartProxy(masterServerId);
            try {
                st = connection.createStatement();
                // will try a ping, if ok, if not, transaction is considered be lost
                st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
            } catch (SQLException e) {
                fail();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void testFailNotOnSlave() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&autoReconnectMaster=true", true);
            Statement stmt = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            try {
                stmt.execute("SELECT 1");
                fail();
            } catch (SQLException e) {
                //normal error
            }
            assertTrue(!connection.isReadOnly());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
