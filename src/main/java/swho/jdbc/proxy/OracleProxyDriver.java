package swho.jdbc.proxy;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A proxy JDBC driver designed to intercept specific Connection & MetaData calls.
 */
public class OracleProxyDriver implements Driver {

    private static final String URL_PREFIX = "jdbc:swhoproxy:";
    private static final Logger LOGGER = Logger.getLogger(OracleProxyDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new OracleProxyDriver());
            LOGGER.info("OracleProxyDriver registered successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to register OracleProxyDriver");
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String realUrl = url.replaceFirst(URL_PREFIX, "jdbc:");
        LOGGER.info("OracleProxyDriver intercepting connection. Forwarding to: " + realUrl);
        
        // Get the real connection from the underlying driver (e.g. Oracle)
        Connection realConnection = DriverManager.getConnection(realUrl, info);
        
        // Wrap the real connection with the proxy
        return ConnectionProxyHandler.createProxy(realConnection);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return new DriverPropertyInfo[0];
        String realUrl = url.replaceFirst(URL_PREFIX, "jdbc:");
        Driver driver = DriverManager.getDriver(realUrl);
        if (driver != null) {
            return driver.getPropertyInfo(realUrl, info);
        }
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 2;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger("swho.jdbc.proxy");
    }
}
