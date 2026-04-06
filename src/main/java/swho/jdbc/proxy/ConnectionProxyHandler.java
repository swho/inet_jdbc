package swho.jdbc.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;

public class ConnectionProxyHandler implements InvocationHandler {

    private final Connection realConnection;

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
            // 回傳經過代理包裝過的 DatabaseMetaData Proxy
            return DatabaseMetaDataProxyHandler.createProxy(realConnection.getMetaData());
        }

        // 其餘方法直接放行到底層的 Oracle Connection
        return method.invoke(realConnection, args);
    }
}
