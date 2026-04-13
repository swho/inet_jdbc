package swho.jdbc.proxy;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class OracleProxyDriverTest {

    @Test
    public void testAcceptsNativeOracleUrls() throws SQLException {
        OracleProxyDriver driver = new OracleProxyDriver();

        assertTrue(driver.acceptsURL("jdbc:oracle:thin:@localhost:1521/ORCL"));
    }

    @Test
    public void testConnectDelegatesNativeOracleUrlsToUnderlyingDriver() throws Exception {
        RecordingOracleDriver delegate = new RecordingOracleDriver();
        DriverManager.registerDriver(delegate);

        try {
            OracleProxyDriver driver = new OracleProxyDriver();
            Connection realConnection = mock(Connection.class);
            delegate.connectionToReturn = realConnection;

            Properties properties = new Properties();
            properties.setProperty("user", "scott");
            properties.setProperty("password", "tiger");

            Connection proxyConnection = driver.connect("jdbc:oracle:thin:@localhost:1521/ORCL", properties);

            assertNotNull(proxyConnection);
            assertEquals("jdbc:oracle:thin:@localhost:1521/ORCL", delegate.lastConnectUrl);
            proxyConnection.close();
            verify(realConnection).close();
        } finally {
            DriverManager.deregisterDriver(delegate);
        }
    }

    @Test
    public void testGetPropertyInfoUsesUnderlyingDriverForNativeOracleUrls() throws Exception {
        OracleProxyDriver driver = new OracleProxyDriver();

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(
                "jdbc:oracle:thin:@localhost:1521/ORCL",
                new Properties()
        );

        assertTrue(propertyInfo.length > 0);
    }

    private static final class RecordingOracleDriver implements Driver {
        private Connection connectionToReturn;
        private DriverPropertyInfo[] propertyInfoToReturn = new DriverPropertyInfo[0];
        private String lastConnectUrl;
        private String lastPropertyInfoUrl;

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            lastConnectUrl = url;
            return connectionToReturn;
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return url != null && url.startsWith("jdbc:oracle:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            lastPropertyInfoUrl = url;
            return propertyInfoToReturn;
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return true;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return Logger.getLogger("test");
        }
    }
}
