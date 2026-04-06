package swho.jdbc.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;

public class EmptyResultSetHandler implements InvocationHandler {

    public static ResultSet createProxy() {
        return (ResultSet) Proxy.newProxyInstance(
                EmptyResultSetHandler.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new EmptyResultSetHandler()
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        
        switch (methodName) {
            case "next":
                return false;
            case "close":
                return null;
            case "isClosed":
                return true;
            case "getMetaData":
                // Instead of null, some designers will NPE if MetaData is null
                // But normally they call next() first, which returns false, so they leave.
                return null;
            case "findColumn":
                throw new java.sql.SQLException("Column not found in empty ResultSet");
        }

        // Return safe defaults for other methods based on return type
        Class<?> returnType = method.getReturnType();
        if (returnType == boolean.class) return false;
        if (returnType == int.class || returnType == short.class || returnType == byte.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == float.class) return 0.0f;
        if (returnType == double.class) return 0.0d;
        
        return null;
    }
}
