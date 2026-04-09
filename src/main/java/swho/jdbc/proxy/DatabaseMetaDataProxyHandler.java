package swho.jdbc.proxy;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;

public class DatabaseMetaDataProxyHandler implements InvocationHandler {

    private final DatabaseMetaData realMetaData;
    private final String currentUserName;
    private static final String LOG_FILE = System.getProperty("user.home") + File.separator + "swho_proxy_metadata.log";

    public DatabaseMetaDataProxyHandler(DatabaseMetaData realMetaData) {
        this.realMetaData = realMetaData;
        String user = null;
        try {
            user = realMetaData.getUserName();
        } catch (Exception e) {
            // Ignore
        }
        this.currentUserName = (user != null) ? user.toUpperCase() : null;
        logToFile("--- MetaData Proxy Created [User: " + currentUserName + "] ---");
    }

    public static DatabaseMetaData createProxy(DatabaseMetaData realMetaData) {
        return (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaDataProxyHandler.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                new DatabaseMetaDataProxyHandler(realMetaData)
        );
    }

    private synchronized void logToFile(String message) {
        try (PrintWriter out = new PrintWriter(new FileWriter(LOG_FILE, true))) {
            out.println(new java.util.Date() + " | " + message);
        } catch (Exception e) {
            // Ignore log errors
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        
        // 1. 特殊處理攔截邏輯
        if (args != null && args.length >= 3 && currentUserName != null) {
            Object schemaPattern = args[1];
            String namePattern = (String) args[2];

            // 全域掃描攔截：當名稱與 Schema 皆為萬用字元時
            if (schemaPattern == null || "%".equals(schemaPattern)) {
                if (namePattern == null || "%".equals(namePattern) || namePattern.contains("%") || namePattern.contains("_")) {
                    
                    // A. 針對 getTables：合併 CurrentUser + SYS (確保 DUAL 可見)
                    if ("getTables".equals(methodName)) {
                        logToFile("Called (Merging CK+SYS) " + methodName + " | Args: " + Arrays.toString(args));
                        long startTime = System.currentTimeMillis();
                        try {
                            List<ResultSet> results = new ArrayList<>();
                            // 呼叫 1: 當前使用者
                            args[1] = currentUserName;
                            results.add((ResultSet) method.invoke(realMetaData, args));
                            // 呼叫 2: SYS 資料表 (Oracle 中查詢 SYS 的表通常較快)
                            args[1] = "SYS";
                            results.add((ResultSet) method.invoke(realMetaData, args));
                            
                            return MergedResultSetHandler.createProxy(results);
                        } finally {
                            long duration = System.currentTimeMillis() - startTime;
                            logToFile("Finished (Merged) getTables | Duration: " + duration + "ms");
                        }
                    }

                    // B. 其他全域掃描：強制限制在當前使用者
                    logToFile("Called (Optimized) " + methodName + " | Args: " + Arrays.toString(args));
                    args[1] = currentUserName;
                } else {
                    logToFile("Called (Pass-through) " + methodName + " | Args: " + Arrays.toString(args));
                }
            } else {
                logToFile("Called " + methodName + " | Args: " + Arrays.toString(args));
            }
        } else {
            // 記錄所有其他呼叫 (包含不帶參數的方法)
            logToFile("Called " + methodName + (args != null ? " | Args: " + Arrays.toString(args) : " | No Args"));
        }

        // 2. 執行呼叫與耗時計時
        long startTime = System.currentTimeMillis();
        try {
            return method.invoke(realMetaData, args);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            logToFile("Finished " + methodName + " | Duration: " + duration + "ms");
        }
    }
}
