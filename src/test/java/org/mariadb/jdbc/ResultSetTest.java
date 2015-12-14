package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class ResultSetTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("result_set_test", "id int not null primary key auto_increment, name char(20)");
    }

    @Test
    public void checkColNumber() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        if (resultSet.next()) {
            try {
                resultSet.getInt(0);
                fail("No column 0, must have thrown error !");
            } catch (SQLException e) {
                //normal Exception
            }
            Assert.assertEquals(resultSet.getInt(1), 1);
            Assert.assertEquals(resultSet.getString(2), "row1");
            try {
                resultSet.getString(3);
                fail("No column 3, must have thrown error !");
            } catch (SQLException e) {
                //normal Exception
            }
        } else {
            fail("Must have one row");
        }
    }

    @Test
    public void isClosedTest() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isClosed());
        while (resultSet.next()) {
            assertFalse(resultSet.isClosed());
        }
        assertFalse(resultSet.isClosed());
        resultSet.close();
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void isBeforeFirstTest() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertTrue(resultSet.isBeforeFirst());
        while (resultSet.next()) {
            assertFalse(resultSet.isBeforeFirst());
        }
        assertFalse(resultSet.isBeforeFirst());
        resultSet.close();
        resultSet.isBeforeFirst();
    }


    @Test
    public void isBeforeFirstTestStream() throws SQLException {
        insertRows(1);
        Statement statement = sharedConnection.createStatement();
        statement.setFetchSize(1);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
        assertTrue(resultSet.isBeforeFirst());
        while (resultSet.next()) {
            assertFalse(resultSet.isBeforeFirst());
        }
        assertFalse(resultSet.isBeforeFirst());
        resultSet.close();
        resultSet.isBeforeFirst();
    }

    @Test
    public void isFirstZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        assertFalse(resultSet.next()); //No more rows after this
        assertFalse(resultSet.isFirst()); // connectorj compatibility
        resultSet.close();
        resultSet.isFirst();
    }

    @Test
    public void isFirstTwoRowsTest() throws SQLException {
        insertRows(2);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        resultSet.next();
        assertTrue(resultSet.isFirst());
        resultSet.next();
        assertFalse(resultSet.isFirst());
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isFirst());
        resultSet.close();
        resultSet.isFirst();
    }

    @Test
    public void isLastZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isLast()); // connectorj compatibility
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isLast());
        resultSet.close();
        resultSet.isLast();
    }

    @Test
    public void isLastZeroRowsTest2() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isLast());
        assertTrue(resultSet.next());
        assertTrue(resultSet.isLast());
        assertFalse(resultSet.next());
        assertFalse(resultSet.isLast());
        assertTrue(resultSet.isAfterLast());
    }

    @Test
    public void isLastZeroRowsTestStreaming() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isLast());
        assertTrue(resultSet.next());
        assertTrue(resultSet.isLast());
        assertFalse(resultSet.next());
        assertFalse(resultSet.isLast());
        assertTrue(resultSet.isAfterLast());
    }

    @Test
    public void isLastTwoRowsTest() throws SQLException {
        insertRows(2);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isLast());
        resultSet.next();
        assertFalse(resultSet.isLast());
        resultSet.next();
        assertTrue(resultSet.isLast());
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isLast());
        resultSet.close();
        resultSet.isLast();
    }

    @Test
    public void isAfterLastZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isAfterLast());
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isAfterLast());
        resultSet.close();
        resultSet.isAfterLast();
    }

    @Test
    public void isAfterLastTwoRowsTest() throws SQLException {
        insertRows(2);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isAfterLast());
        assertTrue(resultSet.next());
        assertFalse(resultSet.isAfterLast());
        assertTrue(resultSet.next());
        assertFalse(resultSet.isAfterLast());
        assertFalse(resultSet.next());
        assertTrue(resultSet.isAfterLast());
        resultSet.close();
        resultSet.isAfterLast();
    }

    @Test
    public void previousTest() throws SQLException {
        insertRows(2);
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(rs.previous());
        assertTrue(rs.next());
        assertFalse(rs.previous());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.previous());
        rs.close();
    }

    @Test
    public void firstTest() throws SQLException {
        insertRows(2);
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.first());
        assertTrue(rs.isFirst());
        rs.close();
        rs.first();
    }

    @Test
    public void lastTest() throws SQLException {
        insertRows(2);
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertTrue(rs.last());
        assertTrue(rs.isLast());
        assertFalse(rs.next());
        rs.first();
        rs.close();
        rs.last();
    }

    private void insertRows(int numberOfRowsToInsert) throws SQLException {
        sharedConnection.createStatement().execute("truncate result_set_test ");
        for (int i = 1; i <= numberOfRowsToInsert; i++) {
            sharedConnection.createStatement().executeUpdate("INSERT INTO result_set_test VALUES(" + i
                    + ", 'row" + i + "')");
        }
    }
}
