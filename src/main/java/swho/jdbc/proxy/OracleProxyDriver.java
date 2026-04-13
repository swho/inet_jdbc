package swho.jdbc.proxy;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A proxy JDBC driver designed to intercept specific Connection & MetaData calls.
 */
public class OracleProxyDriver implements Driver {

    private static final String PROXY_URL_PREFIX = "jdbc:swhoproxy:";
    private static final String ORACLE_URL_PREFIX = "jdbc:oracle:";
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
        String realUrl = toRealUrl(url);
        LOGGER.info("OracleProxyDriver intercepting connection. Forwarding to: " + realUrl);

        Driver delegateDriver = findDelegateDriver(realUrl);
        Connection realConnection = delegateDriver.connect(realUrl, info);
        if (realConnection == null) {
            throw new SQLException("Underlying Oracle JDBC driver refused URL: " + realUrl);
        }

        // Wrap the real connection with the proxy
        return ConnectionProxyHandler.createProxy(realConnection);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null
                && (url.startsWith(PROXY_URL_PREFIX) || url.startsWith(ORACLE_URL_PREFIX));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return new DriverPropertyInfo[0];
        String realUrl = toRealUrl(url);
        return findDelegateDriver(realUrl).getPropertyInfo(realUrl, info);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 3;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger("swho.jdbc.proxy");
    }

    private String toRealUrl(String url) {
        if (url.startsWith(PROXY_URL_PREFIX)) {
            return url.replaceFirst(PROXY_URL_PREFIX, "jdbc:");
        }
        return url;
    }

    private Driver findDelegateDriver(String realUrl) throws SQLException {
        Driver delegateDriver = findRegisteredDelegateDriver(realUrl);
        if (delegateDriver != null) {
            return delegateDriver;
        }

        loadOracleDriverClass("oracle.jdbc.OracleDriver");
        delegateDriver = findRegisteredDelegateDriver(realUrl);
        if (delegateDriver != null) {
            return delegateDriver;
        }

        loadOracleDriverClass("oracle.jdbc.driver.OracleDriver");
        delegateDriver = findRegisteredDelegateDriver(realUrl);
        if (delegateDriver != null) {
            return delegateDriver;
        }

        throw new SQLException("No underlying Oracle JDBC driver found for URL: " + realUrl);
    }

    private Driver findRegisteredDelegateDriver(String realUrl) throws SQLException {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver instanceof OracleProxyDriver) {
                continue;
            }

            if (driver.acceptsURL(realUrl)) {
                return driver;
            }
        }
        return null;
    }

    private void loadOracleDriverClass(String className) {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException ignored) {
            // Ignore and keep looking for already-registered drivers.
        }
    }
}
