package swho.jdbc.proxy;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
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
}
