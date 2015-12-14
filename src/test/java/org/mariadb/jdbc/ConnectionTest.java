package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executor;

import static org.junit.Assert.*;

public class ConnectionTest extends BaseTest {

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("dummy", "a BLOB");
    }

    /**
     * Conj-166.
     * Connection error code must be thrown
     *
     * @throws SQLException exception
     */
    @Test
    public void testAccessDeniedErrorCode() throws SQLException {
        try {
            DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + database + "?user=foo");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue("28000".equals(e.getSQLState()));
            Assert.assertTrue(1045 == e.getErrorCode());
            setConnection();
        }
    }

    /**
     * Conj-89.
     *
     * @throws SQLException exception
     */
    @Test
    public void getPropertiesTest() throws SQLException {
        String params = "&useFractionalSeconds=true&allowMultiQueries=true&useCompression=false";
        Connection connection = null;
        try {
            connection = setConnection(params);
            Properties properties = connection.getClientInfo();
            assertTrue(properties != null);
            assertTrue(properties.size() > 0);
            assertTrue(properties.getProperty("user").equalsIgnoreCase(username));
            // assertTrue(properties.getProperty("password").equalsIgnoreCase(password));
            assertTrue(properties.getProperty("useFractionalSeconds").equalsIgnoreCase("true"));
            assertTrue(properties.getProperty("allowMultiQueries").equalsIgnoreCase("true"));
            assertTrue(properties.getProperty("useCompression").equalsIgnoreCase("false"));
            assertEquals(username, connection.getClientInfo("user"));
            // assertEquals(null, connection.getClientInfo("password"));
            assertEquals("true", connection.getClientInfo("useFractionalSeconds"));
            assertEquals("true", connection.getClientInfo("allowMultiQueries"));
            assertEquals("false", connection.getClientInfo("useCompression"));
        } finally {
            connection.close();
        }
    }

    /**
     * Conj-75 (corrected with CONJ-156)
     * Needs permission java.sql.SQLPermission "abort" or will be skipped.
     *
     * @throws SQLException exception
     */
    @Test
    public void abortTest() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection();
            Statement stmt = connection.createStatement();
            SQLPermission sqlPermission = new SQLPermission("callAbort");

            SecurityManager securityManager = System.getSecurityManager();
            if (securityManager != null && sqlPermission != null) {
                try {
                    securityManager.checkPermission(sqlPermission);
                } catch (SecurityException se) {
                    System.out.println("test 'abortTest' skipped  due to missing policy");
                    return;
                }
            }
            Executor executor = new Executor() {
                @Override
                public void execute(Runnable command) {
                    command.run();
                }
            };
            connection.abort(executor);
            assertTrue(connection.isClosed());
            try {
                stmt.executeQuery("SELECT 1");
                assertTrue(false);
            } catch (SQLException sqle) {
                assertTrue(true);
            } finally {
                stmt.close();
            }
        } finally {
            connection.close();
        }
    }

    /**
     * Conj-121: implemented Connection.getNetworkTimeout and Connection.setNetworkTimeout.
     *
     * @throws SQLException exception
     */
    @Test
    public void networkTimeoutTest() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection();
            int timeout = 1000;
            SQLPermission sqlPermission = new SQLPermission("setNetworkTimeout");
            SecurityManager securityManager = System.getSecurityManager();
            if (securityManager != null && sqlPermission != null) {
                try {
                    securityManager.checkPermission(sqlPermission);
                } catch (SecurityException se) {
                    System.out.println("test 'setNetworkTimeout' skipped  due to missing policy");
                    return;
                }
            }
            Executor executor = new Executor() {
                @Override
                public void execute(Runnable command) {
                    command.run();
                }
            };
            try {
                connection.setNetworkTimeout(executor, timeout);
            } catch (SQLException sqlex) {
                assertTrue(false);
            }
            try {
                int networkTimeout = connection.getNetworkTimeout();
                assertEquals(timeout, networkTimeout);
            } catch (SQLException sqlex) {
                assertTrue(false);
            }
            try {
                connection.createStatement().execute("select sleep(2)");
                fail("Network timeout is " + timeout / 1000 + "sec, but slept for 2sec");
            } catch (SQLException sqlex) {
                assertTrue(connection.isClosed());
            }
        } finally {
            connection.close();
        }
    }

    /**
     * Conj-120 Fix Connection.isValid method.
     *
     * @throws SQLException exception
     */
    @Test
    public void isValidShouldThrowExceptionWithNegativeTimeout() throws SQLException {
        try {
            sharedConnection.isValid(-1);
            fail("The above row should have thrown an SQLException");
        } catch (SQLException sqlex) {
            assertTrue(sqlex.getMessage().contains("negative"));
        }
    }

    /**
     * Conj-116: Make SQLException prettier when too large stream is sent to the server.
     *
     * @throws SQLException exception
     * @throws UnsupportedEncodingException exception
     */
    @Test
    public void checkMaxAllowedPacket() throws Throwable, SQLException, UnsupportedEncodingException {
        Statement statement = sharedConnection.createStatement();
        ResultSet rs = statement.executeQuery("show variables like 'max_allowed_packet'");
        rs.next();
        int maxAllowedPacket = rs.getInt(2);

        //Create a SQL stream bigger than maxAllowedPacket
        StringBuilder sb = new StringBuilder();
        String rowData = "('this is a dummy row values')";
        int rowsToWrite = (maxAllowedPacket / rowData.getBytes("UTF-8").length) + 1;
        try {
            for (int row = 1; row <= rowsToWrite; row++) {
                if (row >= 2) {
                    sb.append(", ");
                }
                sb.append(rowData);
            }
            statement.executeUpdate("INSERT INTO dummy VALUES " + sb.toString());
            fail("The previous statement should throw an SQLException");
        } catch (OutOfMemoryError e) {
            System.out.println("skip test 'maxAllowedPackedExceptionIsPrettyTest' - not enough memory");
            Assume.assumeNoException(e);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("max_allowed_packet"));
        } catch (Exception e) {
            fail("The previous statement should throw an SQLException not a general Exception");
        }

        statement.execute("select count(*) from dummy"); //check that the connection is still working

        //added in CONJ-151 to check the 2 differents type of query implementation
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("INSERT INTO dummy VALUES (?)");
        try {
            byte[] arr = new byte[maxAllowedPacket + 1000];
            Arrays.fill(arr, (byte) 'a');
            preparedStatement.setBytes(1, arr);
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            fail("The previous statement should throw an SQLException");
        } catch (OutOfMemoryError e) {
            System.out.println("skip second test 'maxAllowedPackedExceptionIsPrettyTest' - not enough memory");
            Assume.assumeNoException(e);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("max_allowed_packet"));
        } catch (Exception e) {
            fail("The previous statement should throw an SQLException not a general Exception");
        } finally {
            statement.execute("select count(*) from dummy"); //to check that connection is open
        }
    }


    @Test
    public void isValidTestWorkingConnection() throws SQLException {
        assertTrue(sharedConnection.isValid(0));
    }

    /**
     * CONJ-120 Fix Connection.isValid method
     *
     * @throws SQLException exception
     */
    @Test
    public void isValidClosedConnection() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection();
            connection.close();
            boolean isValid = connection.isValid(0);
            assertFalse(isValid);
        } finally {
            connection.close();
        }
    }

    /**
     * CONJ-120 Fix Connection.isValid method
     *
     * @throws SQLException exception
     * @throws InterruptedException exception
     */
    @Test
    public void isValidConnectionThatTimesOutByServer() throws SQLException, InterruptedException {
        Connection connection = null;
        try {
            connection = setConnection();
            Statement statement = connection.createStatement();
            statement.execute("set session wait_timeout=1");
            Thread.sleep(3000); // Wait for the server to kill the connection
            boolean isValid = connection.isValid(0);
            assertFalse(isValid);
            statement.close();
        } finally {
            connection.close();
        }
    }

}
