package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MoreResultTest extends BaseTest {

    /**
     * Tables initialisation.
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("MoreResultTestT1", "id int, test varchar(100)");
        createTable("MoreResultTestT2", "id int, test varchar(100)");
        createTable("MoreResultTestT5", "id int, test varchar(100)");
        Statement st = sharedConnection.createStatement();
        st.execute("insert into MoreResultTestT1 values(1,'a'),(2,'a')");
        st.execute("insert into MoreResultTestT2 values(1,'a'),(2,'a')");
        st.execute("insert into MoreResultTestT5 values(1,'a'),(2,'a'),(2,'b')");
    }

    /* CONJ-14
     * getUpdateCount(), getResultSet() should indicate "no more results" with
     * (getUpdateCount() == -1 && getResultSet() == null)
    */
    @Test
    public void conj14() throws Exception {
        Statement st = sharedConnection.createStatement();

        /* 1. Test update statement */
        st.execute("use " + database);
        assertEquals(0, st.getUpdateCount());

        /* No more results */
        assertFalse(st.getMoreResults());
        assertEquals(-1, st.getUpdateCount());
        assertEquals(null, st.getResultSet());

        /* 2. Test select statement */
        st.execute("select 1");
        assertEquals(-1, st.getUpdateCount());
        assertTrue(st.getResultSet() != null);

        /* No More results */
        assertFalse(st.getMoreResults());
        assertEquals(-1, st.getUpdateCount());
        assertEquals(null, st.getResultSet());

        /* Test batch  */
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            st = connection.createStatement();

            /* 3. Batch with two SELECTs */

            st.execute("select 1;select 2;select 3");
            /* First result (select)*/
            assertEquals(-1, st.getUpdateCount());
            assertTrue(st.getResultSet() != null);

            /* has more results */
            assertTrue(st.getMoreResults());

            /* Second result (select) */
            assertEquals(-1, st.getUpdateCount());
            assertTrue(st.getResultSet() != null);

            /* has more results */
            assertTrue(st.getMoreResults());

            /* third result (select) */
            assertEquals(-1, st.getUpdateCount());
            assertTrue(st.getResultSet() != null);

            /* no more results */
            assertFalse(st.getMoreResults());
            assertEquals(-1, st.getUpdateCount());
            assertEquals(null, st.getResultSet());

            /* 4. Batch with a SELECT and non-SELECT */

            st.execute("select 1; use " + database);
            /* First result (select)*/
            assertEquals(-1, st.getUpdateCount());
            assertTrue(st.getResultSet() != null);

            /* has more results */
            assertTrue(st.getMoreResults());

            /* Second result (use) */
            assertEquals(0, st.getUpdateCount());
            assertTrue(st.getResultSet() == null);

            /* no more results */
            assertFalse(st.getMoreResults());
            assertEquals(-1, st.getUpdateCount());
            assertEquals(null, st.getResultSet());
        } finally {
            connection.close();
        }
    }

    @Test
    public void basicTest() throws SQLException {
        log.debug("basicTest begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from MoreResultTestT1;select * from MoreResultTestT2;");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count > 0);
            assertTrue(statement.getMoreResults());
            rs = statement.getResultSet();
            count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count > 0);
            assertFalse(statement.getMoreResults());
            log.debug("basicTest end");
        } finally {
            connection.close();
        }
    }


    @Test
    public void setMaxRowsMulti() throws Exception {
        log.debug("setMaxRowsMulti begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement st = connection.createStatement();
            assertEquals(0, st.getMaxRows());

            st.setMaxRows(1);
            assertEquals(1, st.getMaxRows());

            /* Check 3 rows are returned if maxRows is limited to 3, in every result set in batch */

           /* Check first result set for at most 3 rows*/
            ResultSet rs = st.executeQuery("select 1 union select 2;select 1 union select 2");
            int cnt = 0;

            while (rs.next()) {
                cnt++;
            }
            rs.close();
            assertEquals(1, cnt);

           /* Check second result set for at most 3 rows*/
            assertTrue(st.getMoreResults());
            rs = st.getResultSet();
            cnt = 0;
            while (rs.next()) {
                cnt++;
            }
            rs.close();
            assertEquals(1, cnt);
            log.debug("setMaxRowsMulti end");
        } finally {
            connection.close();
        }
    }

    @Test
    public void updateTest() throws SQLException {
        log.debug("updateTest begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            statement.execute("update MoreResultTestT5 set test='a " + System.currentTimeMillis()
                    + "' where id = 2;select * from MoreResultTestT2;");
            int updateNb = statement.getUpdateCount();
            log.debug("statement.getUpdateCount() " + updateNb);
            assertTrue(updateNb == 2);
            assertTrue(statement.getMoreResults());
            ResultSet rs = statement.getResultSet();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count > 0);
            assertFalse(statement.getMoreResults());
            log.debug("updateTest end");
        } finally {
            connection.close();
        }
    }

    @Test
    public void updateTest2() throws SQLException {
        log.debug("updateTest2 begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            statement.execute("select * from MoreResultTestT2;update MoreResultTestT5 set test='a " + System.currentTimeMillis()
                    + "' where id = 2;");
            ResultSet rs = statement.getResultSet();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count == 2);
            statement.getMoreResults();

            int updateNb = statement.getUpdateCount();
            log.debug("statement.getUpdateCount() " + updateNb);
            assertEquals(2, updateNb);
            log.debug("updateTest2 end");
        } finally {
            connection.close();
        }
    }

}
