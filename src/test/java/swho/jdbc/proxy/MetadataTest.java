package swho.jdbc.proxy;

import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class MetadataTest {

    @Test
    public void testGetTablesInterception() throws SQLException {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        
        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        
        // Call getTables with null schema
        proxy.getTables(null, null, "TABLE_NAME", null);
        
        // Verify that the mock was called with TESTUSER schema
        verify(mockMetaData).getTables(null, "TESTUSER", "TABLE_NAME", null);
    }
    
    @Test
    public void testGetTablesDoesNotOverwriteExplicitSchema() throws SQLException {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        
        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        
        // Call getTables with EXPLICIT schema
        proxy.getTables(null, "OTHER_SCHEMA", "TABLE_NAME", null);
        
        // Should NOT overwrite it
        verify(mockMetaData).getTables(null, "OTHER_SCHEMA", "TABLE_NAME", null);
    }

    @Test
    public void testGetTablesDoesNotOverwriteWhenSpecificTableNameProvided() throws SQLException {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        
        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        
        // Call getTables with null schema but SPECIFIC table name
        proxy.getTables(null, null, "DUAL", null);
        
        // Should NOT overwrite it, allowing it to find SYS.DUAL
        verify(mockMetaData).getTables(null, null, "DUAL", null);
    }

    @Test
    public void testGetTablesOverwritesWhenWildcardUsed() throws SQLException {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        
        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        
        // Call getTables with null schema and WILDCARD
        proxy.getTables(null, null, "MY_%", null);
        
        // SHOULD overwrite it for performance
        verify(mockMetaData).getTables(null, "TESTUSER", "MY_%", null);
    }

    @Test
    public void testGetTablesOverwritesWhenTableNameIsNull() throws SQLException {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        
        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        
        // Call getTables with null schema and null table name
        proxy.getTables(null, null, null, null);
        
        // SHOULD overwrite it for performance
        verify(mockMetaData).getTables(null, "TESTUSER", null, null);
    }

    @Test
    public void testGetProcedureColumnsExpandsSimpleRefCursorToResultColumns() throws Exception {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        when(mockMetaData.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(createSourceResultSet(
                "CREATE OR REPLACE PROCEDURE PROC_WITH_CURSOR (P_CURSOR OUT SYS_REFCURSOR) IS\n" +
                        "BEGIN\n" +
                        "  OPEN P_CURSOR FOR SELECT * FROM TABLE_A;\n" +
                        "END;"
        ));
        when(mockMetaData.getProcedureColumns(null, "TESTUSER", "PROC_WITH_CURSOR", "%"))
                .thenReturn(createProcedureColumnsResultSet(
                        new Object[]{
                                null, "TESTUSER", "PROC_WITH_CURSOR", "P_CURSOR",
                                DatabaseMetaData.procedureColumnOut, Types.REF_CURSOR, "SYS_REFCURSOR",
                                0, 0, 0, 10, DatabaseMetaData.procedureNullable, null, null,
                                0, 0, 0, 1, "YES", "PROC_WITH_CURSOR"
                        }
                ));
        when(mockMetaData.getColumns(null, "TESTUSER", "TABLE_A", "%"))
                .thenReturn(createTableColumnsResultSet(
                        new Object[]{
                                null, "TESTUSER", "TABLE_A", "ID", Types.NUMERIC, "NUMBER",
                                22, 0, 10, DatabaseMetaData.columnNoNulls, null, null,
                                0, 0, 0, 1, "NO"
                        },
                        new Object[]{
                                null, "TESTUSER", "TABLE_A", "NAME", Types.VARCHAR, "VARCHAR2",
                                100, 0, 10, DatabaseMetaData.columnNullable, null, null,
                                0, 0, 100, 2, "YES"
                        }
                ));

        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        ResultSet resultSet = proxy.getProcedureColumns(null, "TESTUSER", "PROC_WITH_CURSOR", "%");

        List<String> resultColumns = new ArrayList<>();
        int totalRows = 0;
        while (resultSet.next()) {
            totalRows++;
            if (resultSet.getInt("COLUMN_TYPE") == DatabaseMetaData.procedureColumnResult) {
                resultColumns.add(resultSet.getString("COLUMN_NAME") + ":" + resultSet.getString("TYPE_NAME"));
            }
        }

        assertEquals(3, totalRows);
        assertEquals(Arrays.asList("ID:NUMBER", "NAME:VARCHAR2"), resultColumns);
        verify(mockMetaData).getColumns(null, "TESTUSER", "TABLE_A", "%");
    }

    @Test
    public void testGetProcedureColumnsLeavesScalarMetadataUnchanged() throws Exception {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        when(mockMetaData.getProcedureColumns(null, "TESTUSER", "PROC_NO_CURSOR", "%"))
                .thenReturn(createProcedureColumnsResultSet(
                        new Object[]{
                                null, "TESTUSER", "PROC_NO_CURSOR", "P_ID",
                                DatabaseMetaData.procedureColumnIn, Types.NUMERIC, "NUMBER",
                                22, 22, 0, 10, DatabaseMetaData.procedureNoNulls, null, null,
                                0, 0, 0, 1, "NO", "PROC_NO_CURSOR"
                        }
                ));

        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        ResultSet resultSet = proxy.getProcedureColumns(null, "TESTUSER", "PROC_NO_CURSOR", "%");

        assertTrue(resultSet.next());
        assertEquals("P_ID", resultSet.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.procedureColumnIn, resultSet.getInt("COLUMN_TYPE"));
        assertFalse(resultSet.next());
        verify(mockMetaData, never()).getConnection();
        verify(mockMetaData, never()).getColumns(anyString(), anyString(), anyString(), anyString());
    }

    @Test
    public void testPrepareCallProvidesSyntheticMetadataForRefCursorProcedure() throws Exception {
        Connection realConnection = mock(Connection.class);
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        CallableStatement realCallable = mock(CallableStatement.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(realConnection.getMetaData()).thenReturn(mockMetaData);
        when(realConnection.prepareCall(anyString())).thenReturn(realCallable);
        when(realConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        when(mockMetaData.getConnection()).thenReturn(realConnection);
        when(realCallable.getMetaData()).thenReturn(null);
        when(mockStatement.executeQuery()).thenReturn(createSourceResultSet(
                "CREATE OR REPLACE PROCEDURE PROC_WITH_CURSOR (P_CURSOR OUT SYS_REFCURSOR) IS\n" +
                        "BEGIN\n" +
                        "  OPEN P_CURSOR FOR SELECT * FROM TABLE_A;\n" +
                        "END;"
        ));
        when(mockMetaData.getProcedureColumns(null, "TESTUSER", "PROC_WITH_CURSOR", "%"))
                .thenReturn(createProcedureColumnsResultSet(
                        new Object[]{
                                null, "TESTUSER", "PROC_WITH_CURSOR", "P_CURSOR",
                                DatabaseMetaData.procedureColumnOut, Types.REF_CURSOR, "SYS_REFCURSOR",
                                0, 0, 0, 10, DatabaseMetaData.procedureNullable, null, null,
                                0, 0, 0, 1, "YES", "PROC_WITH_CURSOR"
                        }
                ));
        when(mockMetaData.getColumns(null, "TESTUSER", "TABLE_A", "%"))
                .thenReturn(createTableColumnsResultSet(
                        new Object[]{
                                null, "TESTUSER", "TABLE_A", "ID", Types.NUMERIC, "NUMBER",
                                22, 0, 10, DatabaseMetaData.columnNoNulls, null, null,
                                0, 0, 0, 1, "NO"
                        },
                        new Object[]{
                                null, "TESTUSER", "TABLE_A", "NAME", Types.VARCHAR, "VARCHAR2",
                                100, 0, 10, DatabaseMetaData.columnNullable, null, null,
                                0, 0, 100, 2, "YES"
                        }
                ));

        Connection proxy = ConnectionProxyHandler.createProxy(realConnection);
        CallableStatement callable = proxy.prepareCall("{call PROC_WITH_CURSOR(?)}");
        ResultSetMetaData metaData = callable.getMetaData();

        assertNotNull(metaData);
        assertEquals(2, metaData.getColumnCount());
        assertEquals("ID", metaData.getColumnName(1));
        assertEquals(Types.NUMERIC, metaData.getColumnType(1));
        assertEquals("NAME", metaData.getColumnName(2));
        assertEquals(Types.VARCHAR, metaData.getColumnType(2));
    }

    @Test
    public void testPrepareCallKeepsUnderlyingMetadataWhenAvailable() throws Exception {
        Connection realConnection = mock(Connection.class);
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        CallableStatement realCallable = mock(CallableStatement.class);
        ResultSetMetaData existingMetaData = mock(ResultSetMetaData.class);

        when(realConnection.getMetaData()).thenReturn(mockMetaData);
        when(realConnection.prepareCall(anyString())).thenReturn(realCallable);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        when(realCallable.getMetaData()).thenReturn(existingMetaData);

        Connection proxy = ConnectionProxyHandler.createProxy(realConnection);
        CallableStatement callable = proxy.prepareCall("{call PROC_WITH_METADATA(?)}");

        assertSame(existingMetaData, callable.getMetaData());
        verify(mockMetaData, never()).getProcedureColumns(anyString(), anyString(), anyString(), anyString());
    }

    @Test
    public void testPrepareCallExecuteQueryReturnsRefCursorOutParameterResultSet() throws Exception {
        Connection realConnection = mock(Connection.class);
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        CallableStatement realCallable = mock(CallableStatement.class);
        ResultSet cursorResultSet = mock(ResultSet.class);

        when(realConnection.getMetaData()).thenReturn(mockMetaData);
        when(realConnection.prepareCall(anyString())).thenReturn(realCallable);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        when(realCallable.execute()).thenReturn(false);
        when(realCallable.getObject(1)).thenReturn(cursorResultSet);

        Connection proxy = ConnectionProxyHandler.createProxy(realConnection);
        CallableStatement callable = proxy.prepareCall("{call PROC_WITH_CURSOR(?)}");
        callable.registerOutParameter(1, Types.REF_CURSOR);

        assertSame(cursorResultSet, callable.executeQuery());
        verify(realCallable, never()).executeQuery();
        verify(realCallable).execute();
        verify(realCallable).getObject(1);
    }

    @Test
    public void testPrepareCallExecuteQueryDelegatesWithoutRefCursor() throws Exception {
        Connection realConnection = mock(Connection.class);
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        CallableStatement realCallable = mock(CallableStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(realConnection.getMetaData()).thenReturn(mockMetaData);
        when(realConnection.prepareCall(anyString())).thenReturn(realCallable);
        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        when(realCallable.executeQuery()).thenReturn(resultSet);

        Connection proxy = ConnectionProxyHandler.createProxy(realConnection);
        CallableStatement callable = proxy.prepareCall("{call PROC_NO_CURSOR(?)}");

        assertSame(resultSet, callable.executeQuery());
        verify(realCallable).executeQuery();
        verify(realCallable, never()).execute();
    }

    @Test
    public void testGetProcedureColumnsHandlesMissingOptionalColumnMetadata() throws Exception {
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);

        when(mockMetaData.getUserName()).thenReturn("TESTUSER");
        when(mockMetaData.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(createSourceResultSet(
                "CREATE OR REPLACE PROCEDURE PROC_WITH_CURSOR (P_CURSOR OUT SYS_REFCURSOR) IS\n" +
                        "BEGIN\n" +
                        "  OPEN P_CURSOR FOR SELECT * FROM TABLE_A;\n" +
                        "END;"
        ));
        when(mockMetaData.getProcedureColumns(null, "TESTUSER", "PROC_WITH_CURSOR", "%"))
                .thenReturn(createProcedureColumnsResultSet(
                        new Object[]{
                                null, "TESTUSER", "PROC_WITH_CURSOR", "P_CURSOR",
                                DatabaseMetaData.procedureColumnOut, Types.REF_CURSOR, "SYS_REFCURSOR",
                                0, 0, 0, 10, DatabaseMetaData.procedureNullable, null, null,
                                0, 0, 0, 1, "YES", "PROC_WITH_CURSOR"
                        }
                ));
        when(mockMetaData.getColumns(null, "TESTUSER", "TABLE_A", "%"))
                .thenReturn(createRowSet(
                        new String[]{
                                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                                "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                                "REMARKS", "COLUMN_DEF", "ORDINAL_POSITION", "IS_NULLABLE"
                        },
                        new int[]{
                                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                                Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                                Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR
                        },
                        new Object[][]{
                                new Object[]{
                                        null, "TESTUSER", "TABLE_A", "ID", Types.NUMERIC, "NUMBER",
                                        22, 0, 10, DatabaseMetaData.columnNoNulls, null, null, 1, "NO"
                                }
                        }
                ));

        DatabaseMetaData proxy = DatabaseMetaDataProxyHandler.createProxy(mockMetaData);
        ResultSet resultSet = proxy.getProcedureColumns(null, "TESTUSER", "PROC_WITH_CURSOR", "%");

        assertTrue(resultSet.next());
        assertEquals("P_CURSOR", resultSet.getString("COLUMN_NAME"));
        assertTrue(resultSet.next());
        assertEquals("ID", resultSet.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.procedureColumnResult, resultSet.getInt("COLUMN_TYPE"));
    }

    private static CachedRowSet createProcedureColumnsResultSet(Object[]... rows) throws SQLException {
        return createRowSet(
                new String[]{
                        "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
                        "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE",
                        "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                        "SPECIFIC_NAME"
                },
                new int[]{
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                        Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                        Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR
                },
                rows
        );
    }

    private static CachedRowSet createTableColumnsResultSet(Object[]... rows) throws SQLException {
        return createRowSet(
                new String[]{
                        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                        "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                        "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE"
                },
                new int[]{
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                        Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.INTEGER, Types.INTEGER, Types.VARCHAR
                },
                rows
        );
    }

    private static CachedRowSet createSourceResultSet(String... sourceLines) throws SQLException {
        Object[][] rows = new Object[sourceLines.length][];
        for (int i = 0; i < sourceLines.length; i++) {
            rows[i] = new Object[]{sourceLines[i]};
        }
        return createRowSet(new String[]{"TEXT"}, new int[]{Types.VARCHAR}, rows);
    }

    private static CachedRowSet createRowSet(String[] columnNames, int[] columnTypes, Object[][] rows) throws SQLException {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(columnNames.length);

        for (int i = 0; i < columnNames.length; i++) {
            int columnIndex = i + 1;
            metaData.setColumnName(columnIndex, columnNames[i]);
            metaData.setColumnType(columnIndex, columnTypes[i]);
            metaData.setNullable(columnIndex, ResultSetMetaData.columnNullableUnknown);
        }

        rowSet.setMetaData(metaData);
        for (Object[] row : rows) {
            rowSet.moveToInsertRow();
            for (int i = 0; i < row.length; i++) {
                rowSet.updateObject(i + 1, row[i]);
            }
            rowSet.insertRow();
            rowSet.moveToCurrentRow();
        }
        rowSet.beforeFirst();
        return rowSet;
    }
}
