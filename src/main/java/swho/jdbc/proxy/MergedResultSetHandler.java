package swho.jdbc.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * 負責將多個 ResultSet 合併為一個的代理處理器。
 */
public class MergedResultSetHandler implements InvocationHandler {

    private final List<ResultSet> resultSets;
    private int currentIndex = 0;

    public MergedResultSetHandler(List<ResultSet> resultSets) {
        this.resultSets = resultSets;
    }

    public static ResultSet createProxy(List<ResultSet> resultSets) {
        return (ResultSet) Proxy.newProxyInstance(
                MergedResultSetHandler.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new MergedResultSetHandler(resultSets)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if ("next".equals(methodName)) {
            while (currentIndex < resultSets.size()) {
                if (resultSets.get(currentIndex).next()) {
                    return true;
                }
                currentIndex++;
            }
            return false;
        }

        if ("close".equals(methodName)) {
            for (ResultSet rs : resultSets) {
                try { rs.close(); } catch (Exception e) {}
            }
            return null;
        }

        if ("isClosed".equals(methodName)) {
            return currentIndex >= resultSets.size() || resultSets.get(currentIndex).isClosed();
        }

        if ("getMetaData".equals(methodName)) {
            return resultSets.get(0).getMetaData();
        }

        // 委派讀取操作給目前的 ResultSet
        if (currentIndex < resultSets.size()) {
            return method.invoke(resultSets.get(currentIndex), args);
        }

        // 預設值回傳
        Class<?> returnType = method.getReturnType();
        if (returnType == boolean.class) return false;
        if (returnType == int.class || returnType == long.class || returnType == short.class) return 0;
        return null;
    }
}
