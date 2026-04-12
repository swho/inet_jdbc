package swho.jdbc.proxy;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CallableStatementProxyHandler implements InvocationHandler {

    private static final String LOG_FILE = System.getProperty("user.home") + File.separator + "swho_proxy_metadata.log";
    private static final Pattern CALL_PATTERN = Pattern.compile(
            "(?is)\\bcall\\s+((?:\"?[A-Za-z0-9_$#]+\"?\\.){0,2}\"?[A-Za-z0-9_$#]+\"?)"
    );
    private static final Pattern BEGIN_PATTERN = Pattern.compile(
            "(?is)\\bbegin\\s+((?:\"?[A-Za-z0-9_$#]+\"?\\.){0,2}\"?[A-Za-z0-9_$#]+\"?)\\s*\\("
    );

    private final CallableStatement realCallableStatement;
    private final DatabaseMetaDataProxyHandler metadataSupport;
    private final String sql;
    private final Set<Integer> refCursorIndexes = new LinkedHashSet<>();

    private CallableStatementProxyHandler(
            CallableStatement realCallableStatement,
            DatabaseMetaDataProxyHandler metadataSupport,
            String sql
    ) {
        this.realCallableStatement = realCallableStatement;
        this.metadataSupport = metadataSupport;
        this.sql = sql;
    }

    public static CallableStatement createProxy(
            CallableStatement realCallableStatement,
            Connection realConnection,
            String sql
    ) throws SQLException {
        DatabaseMetaDataProxyHandler metadataSupport = new DatabaseMetaDataProxyHandler(realConnection.getMetaData());
        return (CallableStatement) Proxy.newProxyInstance(
                CallableStatementProxyHandler.class.getClassLoader(),
                new Class<?>[]{CallableStatement.class},
                new CallableStatementProxyHandler(realCallableStatement, metadataSupport, sql)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (shouldLogMethod(methodName)) {
            logToFile("CallableStatement." + methodName +
                    (args != null ? " | Args: " + Arrays.toString(args) : " | No Args") +
                    " | SQL: " + sql);
        }

        if ("registerOutParameter".equals(methodName) && args != null && args.length >= 2
                && args[0] instanceof Integer && args[1] instanceof Integer) {
            int parameterIndex = (Integer) args[0];
            int sqlType = (Integer) args[1];
            if (sqlType == Types.REF_CURSOR || sqlType == -10) {
                refCursorIndexes.add(parameterIndex);
            }
            return method.invoke(realCallableStatement, args);
        }

        if ("getMetaData".equals(methodName)) {
            ResultSetMetaData metadata = (ResultSetMetaData) method.invoke(realCallableStatement, args);
            if (metadata != null) {
                return metadata;
            }

            CallTarget callTarget = parseCallTarget(sql);
            if (callTarget == null) {
                return null;
            }

            ResultSetMetaData syntheticMetaData = metadataSupport.createRefCursorResultSetMetaData(
                    null,
                    callTarget.schemaHint,
                    callTarget.procedureName,
                    callTarget.specificNameHint
            );
            if (syntheticMetaData != null) {
                logToFile("CallableStatement.getMetaData synthesized from REF CURSOR for " + callTarget.procedureName);
                return syntheticMetaData;
            }

            return null;
        }

        if ("executeQuery".equals(methodName) && !refCursorIndexes.isEmpty()) {
            realCallableStatement.execute();
            for (Integer refCursorIndex : refCursorIndexes) {
                Object cursor = realCallableStatement.getObject(refCursorIndex);
                if (cursor instanceof ResultSet) {
                    logToFile("CallableStatement.executeQuery returning REF CURSOR from OUT parameter " + refCursorIndex);
                    return cursor;
                }
            }

            ResultSet resultSet = realCallableStatement.getResultSet();
            if (resultSet != null) {
                return resultSet;
            }

            return null;
        }

        return method.invoke(realCallableStatement, args);
    }

    private boolean shouldLogMethod(String methodName) {
        return "getMetaData".equals(methodName)
                || "execute".equals(methodName)
                || "executeQuery".equals(methodName)
                || "getObject".equals(methodName)
                || "getResultSet".equals(methodName)
                || "registerOutParameter".equals(methodName);
    }

    private CallTarget parseCallTarget(String sql) {
        if (sql == null) {
            return null;
        }

        Matcher callMatcher = CALL_PATTERN.matcher(sql);
        String qualifiedName = null;
        if (callMatcher.find()) {
            qualifiedName = callMatcher.group(1);
        } else {
            Matcher beginMatcher = BEGIN_PATTERN.matcher(sql);
            if (beginMatcher.find()) {
                qualifiedName = beginMatcher.group(1);
            }
        }

        if (qualifiedName == null) {
            return null;
        }

        String[] parts = qualifiedName.split("\\.");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = normalizeIdentifier(parts[i]);
        }

        if (parts.length == 1) {
            return new CallTarget(null, parts[0], null);
        }

        if (parts.length == 2) {
            return new CallTarget(parts[0], parts[1], parts[0] + "." + parts[1]);
        }

        return new CallTarget(parts[0], parts[2], parts[1] + "." + parts[2]);
    }

    private String normalizeIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }

        String trimmed = identifier.trim();
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() > 1) {
            return trimmed.substring(1, trimmed.length() - 1);
        }

        return trimmed.toUpperCase();
    }

    private static synchronized void logToFile(String message) {
        try (PrintWriter out = new PrintWriter(new FileWriter(LOG_FILE, true))) {
            out.println(new java.util.Date() + " | " + message);
        } catch (Exception e) {
            // Ignore log errors
        }
    }

    private static final class CallTarget {
        private final String schemaHint;
        private final String procedureName;
        private final String specificNameHint;

        private CallTarget(String schemaHint, String procedureName, String specificNameHint) {
            this.schemaHint = schemaHint;
            this.procedureName = procedureName;
            this.specificNameHint = specificNameHint;
        }
    }
}
