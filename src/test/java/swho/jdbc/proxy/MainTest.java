package swho.jdbc.proxy;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

public class MainTest {
    public static void main(String[] args) {
        try {
            // JVM should automatically load it via SPI
            Class.forName("swho.jdbc.proxy.OracleProxyDriver");
            // To ensure oracle driver is available in classpath if we need
            // Class.forName("oracle.jdbc.OracleDriver"); 

            // 請根據您的測試環境修改以下連線資訊：
            String testUrl = "jdbc:oracle:thin:@YOUR_ORACLE_HOST:1521/YOUR_SERVICE";
            String proxyUrl = "jdbc:swhoproxy:oracle:thin:@YOUR_ORACLE_HOST:1521/YOUR_SERVICE";
            
            Properties props = new Properties();
            props.put("user", "YOUR_USERNAME");
            props.put("password", "YOUR_PASSWORD");

            System.out.println("=== Phase 2 Performance Comparison Test ===\n");

            // 1. Test original OJDBC
            System.out.println("1) Testing Original Oracle JDBC directly...");
            long startOriginal = System.currentTimeMillis();
            try (Connection connOriginal = DriverManager.getConnection(testUrl, props)) {
                DatabaseMetaData metaData = connOriginal.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    // force iterating slightly to trigger reading
                    if (tables != null && tables.next()) {
                         // found tables
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed original oracle connect (Needs Oracle JDBC driver and DB reachability): " + e.getMessage());
            }
            long oracleTime = System.currentTimeMillis() - startOriginal;

            // 2. Test Proxy Intercept
            System.out.println("2) Testing swho_jdbc proxy...");
            long startProxy = System.currentTimeMillis();
            try (Connection connProxy = DriverManager.getConnection(proxyUrl, props)) {
                DatabaseMetaData metaProxy = connProxy.getMetaData();
                try (ResultSet mockTables = metaProxy.getTables(null, null, "%", new String[]{"TABLE"})) {
                    if (mockTables != null && mockTables.next()) {
                        System.out.println("Proxy got tables? This shouldn't happen!");
                    } else {
                        System.out.println("Proxy immediately returned empty ResultSet as expected.");
                    }
                }

                System.out.println("Verifying typical SQL execution works and schema parsing...");
                try (PreparedStatement pstmt = connProxy.prepareStatement("SELECT * FROM DUAL")) {
                    ResultSetMetaData rsMetaData = pstmt.getMetaData();
                    if (rsMetaData != null) {
                        System.out.println("Successfully got ResultSetMetaData from proxy! Column count: " + rsMetaData.getColumnCount());
                    } else {
                        System.out.println("ResultSetMetaData was null, but SQL prepare statement was wrapped gracefully.");
                        // Sometime Oracle DUAL metadata is null until executed depending on version, test with execute
                    }
                }

            } catch (Exception e) {
                System.out.println("Failed proxy connect (Needs Oracle JDBC driver and DB reachability): " + e.getMessage());
            }
            long proxyTime = System.currentTimeMillis() - startProxy;

            System.out.println("\n=== SUMMARY ===");
            System.out.println("Original ojdbc getTables() Time: " + oracleTime + " ms");
            System.out.println("Proxy getTables() Time: " + proxyTime + " ms");
            System.out.println("Time Saved: " + (oracleTime - proxyTime) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
