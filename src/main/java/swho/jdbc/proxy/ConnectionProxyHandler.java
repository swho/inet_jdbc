package swho.jdbc.proxy;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;

public class ConnectionProxyHandler implements InvocationHandler {

    private final Connection realConnection;
    private static final String LOG_FILE = System.getProperty("user.home") + File.separator + "swho_proxy_metadata.log";

    public ConnectionProxyHandler(Connection realConnection) {
        this.realConnection = realConnection;
    }

    public static Connection createProxy(Connection realConnection) {
        return (Connection) Proxy.newProxyInstance(
                ConnectionProxyHandler.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                new ConnectionProxyHandler(realConnection)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if ("getMetaData".equals(methodName)) {
            // 回傳經過代理包裝過的 DatabaseMetaData Proxy (不進行快取)
            return DatabaseMetaDataProxyHandler.createProxy(realConnection.getMetaData());
        }

        if ("prepareCall".equals(methodName)) {
            String sql = args != null && args.length > 0 ? String.valueOf(args[0]) : null;
            logToFile("Connection.prepareCall | SQL: " + sql);
            CallableStatement callableStatement = (CallableStatement) method.invoke(realConnection, args);
            return CallableStatementProxyHandler.createProxy(callableStatement, realConnection, sql);
        }

        if ("prepareStatement".equals(methodName) && args != null && args.length > 0) {
            logToFile("Connection.prepareStatement | SQL: " + String.valueOf(args[0]));
        }

        // 其餘方法直接放行到底層的 Oracle Connection
        return method.invoke(realConnection, args);
    }

    private static synchronized void logToFile(String message) {
        try (PrintWriter out = new PrintWriter(new FileWriter(LOG_FILE, true))) {
            out.println(new java.util.Date() + " | " + message);
        } catch (Exception e) {
            // Ignore log errors
        }
    }
}
