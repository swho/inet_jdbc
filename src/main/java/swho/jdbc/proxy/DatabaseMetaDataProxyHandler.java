package swho.jdbc.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;

public class DatabaseMetaDataProxyHandler implements InvocationHandler {

    private final DatabaseMetaData realMetaData;

    public DatabaseMetaDataProxyHandler(DatabaseMetaData realMetaData) {
        this.realMetaData = realMetaData;
    }

    public static DatabaseMetaData createProxy(DatabaseMetaData realMetaData) {
        return (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaDataProxyHandler.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                new DatabaseMetaDataProxyHandler(realMetaData)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        // 針對會導致效能問題的結構讀取（Table / Column / Procedure）進行攔截並限定 Schema 範圍
        if ("getTables".equals(methodName) || 
            "getColumns".equals(methodName) || 
            "getSchemas".equals(methodName) || 
            "getCatalogs".equals(methodName) ||
            "getIndexInfo".equals(methodName) ||
            "getPrimaryKeys".equals(methodName) ||
            "getExportedKeys".equals(methodName) ||
            "getImportedKeys".equals(methodName) ||
            "getProcedures".equals(methodName) ||
            "getProcedureColumns".equals(methodName) ||
            "getFunctions".equals(methodName) ||
            "getFunctionColumns".equals(methodName)) {
            
            if (args != null && args.length >= 2) {
                String currentUser = realMetaData.getUserName();
                if (currentUser != null) {
                    currentUser = currentUser.toUpperCase(); 
                    // 將 schemaPattern 強制設定為當前登入使用者，以提升查詢效能並過濾無關資料
                    args[1] = currentUser;
                }
            }
            
            return method.invoke(realMetaData, args);
        }

        // 其餘方法直接委派
        return method.invoke(realMetaData, args);
    }
}
