package swho.jdbc.proxy;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

public class DatabaseMetaDataProxyHandler implements InvocationHandler {

    private final DatabaseMetaData realMetaData;
    private final String currentUserName;
    private static final String LOG_FILE = System.getProperty("user.home") + File.separator + "swho_proxy_metadata.log";
    private static final String SOURCE_SQL =
            "SELECT TEXT FROM ALL_SOURCE WHERE OWNER = ? AND NAME = ? AND TYPE = ? ORDER BY LINE";

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

        if ("getProcedureColumns".equals(methodName)) {
            ResultSet expandedProcedureColumns = maybeExpandProcedureColumns(method, args);
            if (expandedProcedureColumns != null) {
                return expandedProcedureColumns;
            }
        }
        
        // 1. 攔截與優化邏輯
        if (args != null && args.length >= 3 && currentUserName != null) {
            Object schemaPattern = args[1];
            String namePattern = (String) args[2];

            // 判斷是否為「全域掃描」 (Schema 為 null/%，且名稱也是萬用字元或 null)
            if (schemaPattern == null || "%".equals(schemaPattern)) {
                if (namePattern == null || "%".equals(namePattern) || namePattern.contains("%") || namePattern.contains("_")) {
                    
                    // A. 針對 getTables / getColumns：合併「當前使用者」與「SYS.DUAL」
                    // 這樣既能保證速度，又能讓 Designer 看到 DUAL 及其欄位
                    if ("getTables".equals(methodName) || "getColumns".equals(methodName)) {
                        logToFile("Called (Merging " + currentUserName + " + SYS.DUAL) " + methodName + " | Args: " + Arrays.toString(args));
                        long startTime = System.currentTimeMillis();
                        try {
                            List<ResultSet> results = new ArrayList<>();
                            // 查詢 1: 當前使用者的全域物件
                            args[1] = currentUserName;
                            results.add((ResultSet) method.invoke(realMetaData, args));
                            
                            // 查詢 2: SYS 下的 DUAL (精確查詢，速度極快)
                            args[1] = "SYS";
                            args[2] = "DUAL";
                            results.add((ResultSet) method.invoke(realMetaData, args));
                            
                            return MergedResultSetHandler.createProxy(results);
                        } finally {
                            long duration = System.currentTimeMillis() - startTime;
                            logToFile("Finished (Merged) " + methodName + " | Duration: " + duration + "ms");
                        }
                    }

                    // B. 其他全域掃描（如 getProcedures）：強制限制在當前使用者
                    logToFile("Called (Optimized) " + methodName + " | Args: " + Arrays.toString(args));
                    args[1] = currentUserName;
                } else {
                    // 如果名稱是特定的 (如 DUAL 或 XX.XXX)，則完全放行不限制 Schema
                    logToFile("Called (Pass-through) " + methodName + " | Args: " + Arrays.toString(args));
                }
            } else {
                logToFile("Called " + methodName + " | Args: " + Arrays.toString(args));
            }
        } else {
            logToFile("Called " + methodName + (args != null ? " | Args: " + Arrays.toString(args) : " | No Args"));
        }

        // 2. 執行原始呼叫並計時
        long startTime = System.currentTimeMillis();
        try {
            return method.invoke(realMetaData, args);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            logToFile("Finished " + methodName + " | Duration: " + duration + "ms");
        }
    }

    private ResultSet maybeExpandProcedureColumns(Method method, Object[] args) throws Throwable {
        if (args == null || args.length < 4) {
            return null;
        }

        String procedureNamePattern = asString(args[2]);
        if (!isExplicitLookup(procedureNamePattern)) {
            return null;
        }

        logToFile("Called (Resolving REF CURSOR) getProcedureColumns | Args: " + Arrays.toString(args));
        long startTime = System.currentTimeMillis();

        CachedRowSet cachedProcedureColumns;
        try (ResultSet procedureColumns = (ResultSet) method.invoke(realMetaData, args.clone())) {
            cachedProcedureColumns = cacheResultSet(procedureColumns);
        }

        if (cachedProcedureColumns == null) {
            return null;
        }

        try {
            CachedRowSet syntheticResultColumns = buildSyntheticProcedureColumns(cachedProcedureColumns, args);
            cachedProcedureColumns.beforeFirst();

            if (syntheticResultColumns == null || !syntheticResultColumns.next()) {
                cachedProcedureColumns.beforeFirst();
                return cachedProcedureColumns;
            }

            syntheticResultColumns.beforeFirst();
            return MergedResultSetHandler.createProxy(Arrays.<ResultSet>asList(cachedProcedureColumns, syntheticResultColumns));
        } catch (SQLException e) {
            cachedProcedureColumns.beforeFirst();
            logToFile("REF CURSOR expansion skipped for " + procedureNamePattern + " | Reason: " + e.getMessage());
            return cachedProcedureColumns;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            logToFile("Finished getProcedureColumns | Duration: " + duration + "ms");
        }
    }

    ResultSetMetaData createRefCursorResultSetMetaData(
            String procedureCatalog,
            String procedureSchemaHint,
            String procedureName,
            String specificNameHint
    ) throws SQLException {
        RefCursorDescriptor refCursor = findFirstRefCursorDescriptor(
                procedureCatalog,
                procedureSchemaHint,
                procedureName,
                specificNameHint
        );
        if (refCursor == null) {
            return null;
        }

        TableReference tableReference = resolveCursorTable(refCursor);
        if (tableReference == null) {
            return null;
        }

        logToFile("Resolved CallableStatement metadata " + procedureName + " -> " +
                tableReference.schema + "." + tableReference.table);

        try (ResultSet tableColumns = realMetaData.getColumns(procedureCatalog, tableReference.schema, tableReference.table, "%")) {
            return buildResultSetMetaData(tableReference, tableColumns);
        }
    }

    private CachedRowSet buildSyntheticProcedureColumns(CachedRowSet procedureColumns, Object[] args) throws SQLException {
        List<RefCursorDescriptor> refCursorColumns = findRefCursorColumns(procedureColumns);
        if (refCursorColumns.isEmpty()) {
            return null;
        }

        CachedRowSet syntheticRows = createCompatibleRowSet(procedureColumns.getMetaData());
        Map<String, Integer> procedureColumnIndexes = buildColumnIndexMap(procedureColumns.getMetaData());
        String columnNamePattern = asString(args[3]);

        for (RefCursorDescriptor refCursor : refCursorColumns) {
            TableReference tableReference = resolveCursorTable(refCursor);
            if (tableReference == null) {
                continue;
            }

            logToFile("Resolved REF CURSOR " + refCursor.columnName + " -> " +
                    tableReference.schema + "." + tableReference.table);

            try (ResultSet tableColumns = realMetaData.getColumns(
                    refCursor.procedureCatalog,
                    tableReference.schema,
                    tableReference.table,
                    columnNamePattern == null ? "%" : columnNamePattern
            )) {
                Map<String, Integer> tableColumnIndexes = buildColumnIndexMap(tableColumns.getMetaData());
                while (tableColumns.next()) {
                    insertProcedureResultColumn(
                            syntheticRows,
                            procedureColumnIndexes,
                            refCursor,
                            tableColumns,
                            tableColumnIndexes
                    );
                }
            }
        }

        syntheticRows.beforeFirst();
        return syntheticRows;
    }

    private RefCursorDescriptor findFirstRefCursorDescriptor(
            String procedureCatalog,
            String procedureSchemaHint,
            String procedureName,
            String specificNameHint
    ) throws SQLException {
        String normalizedSchemaHint = normalizeIdentifier(procedureSchemaHint);
        List<String> schemaCandidates = new ArrayList<>();
        if (normalizedSchemaHint != null) {
            schemaCandidates.add(normalizedSchemaHint);
        }
        if (currentUserName != null && !currentUserName.equals(normalizedSchemaHint)) {
            schemaCandidates.add(currentUserName);
        }

        if (schemaCandidates.isEmpty()) {
            schemaCandidates.add(currentUserName);
        }

        for (String schemaCandidate : schemaCandidates) {
            RefCursorDescriptor refCursor = loadFirstRefCursorDescriptor(
                    procedureCatalog,
                    schemaCandidate,
                    procedureName,
                    specificNameHint
            );
            if (refCursor != null) {
                return refCursor;
            }
        }

        return null;
    }

    private RefCursorDescriptor loadFirstRefCursorDescriptor(
            String procedureCatalog,
            String procedureSchema,
            String procedureName,
            String specificNameHint
    ) throws SQLException {
        try (ResultSet procedureColumns = realMetaData.getProcedureColumns(
                procedureCatalog,
                procedureSchema,
                procedureName,
                "%"
        )) {
            CachedRowSet cachedProcedureColumns = cacheResultSet(procedureColumns);
            if (cachedProcedureColumns == null) {
                return null;
            }

            List<RefCursorDescriptor> refCursorColumns = findRefCursorColumns(cachedProcedureColumns);
            if (refCursorColumns.isEmpty()) {
                return null;
            }

            RefCursorDescriptor matched = findMatchingRefCursorDescriptor(refCursorColumns, specificNameHint);
            return matched != null ? matched : refCursorColumns.get(0);
        }
    }

    private RefCursorDescriptor findMatchingRefCursorDescriptor(
            List<RefCursorDescriptor> refCursorColumns,
            String specificNameHint
    ) {
        String normalizedSpecificNameHint = normalizeIdentifier(specificNameHint);
        if (normalizedSpecificNameHint == null) {
            return null;
        }

        for (RefCursorDescriptor refCursor : refCursorColumns) {
            String specificName = normalizeIdentifier(refCursor.specificName);
            if (normalizedSpecificNameHint.equals(specificName)) {
                return refCursor;
            }
        }

        return null;
    }

    private List<RefCursorDescriptor> findRefCursorColumns(CachedRowSet procedureColumns) throws SQLException {
        List<RefCursorDescriptor> refCursorColumns = new ArrayList<>();

        while (procedureColumns.next()) {
            int columnType = procedureColumns.getInt("COLUMN_TYPE");
            if (!isResultSetOutputColumnType(columnType) || !isRefCursorType(procedureColumns)) {
                continue;
            }

            refCursorColumns.add(new RefCursorDescriptor(
                    procedureColumns.getString("PROCEDURE_CAT"),
                    defaultSchema(procedureColumns.getString("PROCEDURE_SCHEM")),
                    procedureColumns.getString("PROCEDURE_NAME"),
                    procedureColumns.getString("COLUMN_NAME"),
                    procedureColumns.getString("SPECIFIC_NAME")
            ));
        }

        procedureColumns.beforeFirst();
        return refCursorColumns;
    }

    private boolean isResultSetOutputColumnType(int columnType) {
        return columnType == DatabaseMetaData.procedureColumnOut
                || columnType == DatabaseMetaData.procedureColumnInOut
                || columnType == DatabaseMetaData.procedureColumnReturn
                || columnType == DatabaseMetaData.procedureColumnUnknown;
    }

    private boolean isRefCursorType(ResultSet procedureColumn) throws SQLException {
        int dataType = procedureColumn.getInt("DATA_TYPE");
        String typeName = procedureColumn.getString("TYPE_NAME");

        if (!procedureColumn.wasNull() && (dataType == Types.REF_CURSOR || dataType == -10)) {
            return true;
        }

        return typeName != null && typeName.toUpperCase().contains("CURSOR");
    }

    private TableReference resolveCursorTable(RefCursorDescriptor refCursor) throws SQLException {
        String routineSource = loadRoutineSource(refCursor);
        if (routineSource == null || routineSource.trim().isEmpty()) {
            logToFile("REF CURSOR source not found for " + refCursor.procedureSchema + "." + refCursor.procedureName);
            return null;
        }

        return findCursorTableInSource(routineSource, refCursor);
    }

    private String loadRoutineSource(RefCursorDescriptor refCursor) throws SQLException {
        Connection connection = realMetaData.getConnection();
        if (connection == null) {
            return null;
        }

        String owner = defaultSchema(refCursor.procedureSchema);
        String specificName = normalizeIdentifier(refCursor.specificName);
        String procedureName = normalizeIdentifier(refCursor.procedureName);

        if (specificName != null && specificName.contains(".")) {
            String packageName = specificName.substring(0, specificName.indexOf('.'));
            String packageSource = readSource(connection, owner, packageName, "PACKAGE BODY");
            if (packageSource != null) {
                return packageSource;
            }
        }

        String procedureSource = readSource(connection, owner, procedureName, "PROCEDURE");
        if (procedureSource != null) {
            return procedureSource;
        }

        return readSource(connection, owner, procedureName, "FUNCTION");
    }

    private String readSource(Connection connection, String owner, String objectName, String objectType) throws SQLException {
        if (objectName == null || objectName.isEmpty()) {
            return null;
        }

        StringBuilder source = new StringBuilder();
        try (PreparedStatement statement = connection.prepareStatement(SOURCE_SQL)) {
            statement.setString(1, owner);
            statement.setString(2, objectName);
            statement.setString(3, objectType);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    source.append(resultSet.getString(1)).append('\n');
                }
            }
        }

        return source.length() == 0 ? null : source.toString();
    }

    private TableReference findCursorTableInSource(String routineSource, RefCursorDescriptor refCursor) {
        String cursorName = normalizeIdentifier(refCursor.columnName);
        if (cursorName == null || cursorName.isEmpty()) {
            return null;
        }

        Pattern openCursorPattern = Pattern.compile(
                "(?is)\\bopen\\s+\"?" + Pattern.quote(cursorName) + "\"?\\s+for\\s+select\\s+\\*\\s+from\\s+" +
                        "((?:\"?[A-Za-z0-9_$#]+\"?\\.)?\"?[A-Za-z0-9_$#]+\"?)"
        );
        Matcher matcher = openCursorPattern.matcher(routineSource);
        if (!matcher.find()) {
            logToFile("REF CURSOR source for " + refCursor.columnName + " did not match SELECT * FROM <table>");
            return null;
        }

        String qualifiedTableName = matcher.group(1).trim();
        int separator = qualifiedTableName.indexOf('.');
        if (separator >= 0) {
            return new TableReference(
                    normalizeIdentifier(qualifiedTableName.substring(0, separator)),
                    normalizeIdentifier(qualifiedTableName.substring(separator + 1))
            );
        }

        return new TableReference(defaultSchema(refCursor.procedureSchema), normalizeIdentifier(qualifiedTableName));
    }

    private CachedRowSet cacheResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return null;
        }

        CachedRowSet cachedRowSet = RowSetProvider.newFactory().createCachedRowSet();
        cachedRowSet.populate(resultSet);
        cachedRowSet.beforeFirst();
        return cachedRowSet;
    }

    private ResultSetMetaData buildResultSetMetaData(TableReference tableReference, ResultSet tableColumns) throws SQLException {
        List<ColumnDefinition> columns = new ArrayList<>();
        while (tableColumns.next()) {
            columns.add(new ColumnDefinition(
                    tableColumns.getString("COLUMN_NAME"),
                    tableColumns.getInt("DATA_TYPE"),
                    tableColumns.getString("TYPE_NAME"),
                    tableColumns.getInt("COLUMN_SIZE"),
                    tableColumns.getInt("DECIMAL_DIGITS"),
                    tableColumns.getInt("NULLABLE"),
                    tableColumns.getInt("ORDINAL_POSITION")
            ));
        }

        if (columns.isEmpty()) {
            return null;
        }

        Collections.sort(columns, Comparator.comparingInt(column -> column.ordinalPosition));

        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            int columnIndex = i + 1;
            ColumnDefinition column = columns.get(i);

            rowSetMetaData.setColumnName(columnIndex, column.columnName);
            rowSetMetaData.setColumnLabel(columnIndex, column.columnName);
            rowSetMetaData.setColumnType(columnIndex, column.dataType);
            rowSetMetaData.setColumnTypeName(columnIndex, column.typeName);
            rowSetMetaData.setNullable(columnIndex, column.nullable);
            rowSetMetaData.setPrecision(columnIndex, column.columnSize);
            rowSetMetaData.setScale(columnIndex, column.decimalDigits);
            rowSetMetaData.setSchemaName(columnIndex, tableReference.schema);
            rowSetMetaData.setTableName(columnIndex, tableReference.table);
        }

        return rowSetMetaData;
    }

    private CachedRowSet createCompatibleRowSet(ResultSetMetaData metadata) throws SQLException {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        int columnCount = metadata.getColumnCount();
        rowSetMetaData.setColumnCount(columnCount);

        for (int i = 1; i <= columnCount; i++) {
            rowSetMetaData.setColumnName(i, metadata.getColumnName(i));
            rowSetMetaData.setColumnType(i, metadata.getColumnType(i));
            rowSetMetaData.setNullable(i, metadata.isNullable(i));
            rowSetMetaData.setPrecision(i, metadata.getPrecision(i));
            rowSetMetaData.setScale(i, metadata.getScale(i));
        }

        rowSet.setMetaData(rowSetMetaData);
        return rowSet;
    }

    private Map<String, Integer> buildColumnIndexMap(ResultSetMetaData metadata) throws SQLException {
        Map<String, Integer> columnIndexes = new HashMap<>();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            columnIndexes.put(metadata.getColumnName(i).toUpperCase(), i);
        }
        return columnIndexes;
    }

    private void insertProcedureResultColumn(
            CachedRowSet rowSet,
            Map<String, Integer> columnIndexes,
            RefCursorDescriptor refCursor,
            ResultSet tableColumn,
            Map<String, Integer> tableColumnIndexes
    ) throws SQLException {
        rowSet.moveToInsertRow();

        updateValue(rowSet, columnIndexes, "PROCEDURE_CAT", refCursor.procedureCatalog);
        updateValue(rowSet, columnIndexes, "PROCEDURE_SCHEM", refCursor.procedureSchema);
        updateValue(rowSet, columnIndexes, "PROCEDURE_NAME", refCursor.procedureName);
        updateValue(rowSet, columnIndexes, "COLUMN_NAME", getString(tableColumn, tableColumnIndexes, "COLUMN_NAME"));
        updateValue(rowSet, columnIndexes, "COLUMN_TYPE", DatabaseMetaData.procedureColumnResult);
        updateValue(rowSet, columnIndexes, "DATA_TYPE", getInteger(tableColumn, tableColumnIndexes, "DATA_TYPE"));
        updateValue(rowSet, columnIndexes, "TYPE_NAME", getString(tableColumn, tableColumnIndexes, "TYPE_NAME"));
        updateValue(rowSet, columnIndexes, "PRECISION", getInteger(tableColumn, tableColumnIndexes, "COLUMN_SIZE"));
        updateValue(rowSet, columnIndexes, "LENGTH", getLength(tableColumn, tableColumnIndexes));
        updateValue(rowSet, columnIndexes, "SCALE", getInteger(tableColumn, tableColumnIndexes, "DECIMAL_DIGITS"));
        updateValue(rowSet, columnIndexes, "RADIX", getInteger(tableColumn, tableColumnIndexes, "NUM_PREC_RADIX"));
        updateValue(rowSet, columnIndexes, "NULLABLE", getInteger(tableColumn, tableColumnIndexes, "NULLABLE"));
        updateValue(rowSet, columnIndexes, "REMARKS", getString(tableColumn, tableColumnIndexes, "REMARKS"));
        updateValue(rowSet, columnIndexes, "COLUMN_DEF", getString(tableColumn, tableColumnIndexes, "COLUMN_DEF"));
        updateValue(rowSet, columnIndexes, "SQL_DATA_TYPE", getInteger(tableColumn, tableColumnIndexes, "SQL_DATA_TYPE"));
        updateValue(rowSet, columnIndexes, "SQL_DATETIME_SUB", getInteger(tableColumn, tableColumnIndexes, "SQL_DATETIME_SUB"));
        updateValue(rowSet, columnIndexes, "CHAR_OCTET_LENGTH", getInteger(tableColumn, tableColumnIndexes, "CHAR_OCTET_LENGTH"));
        updateValue(rowSet, columnIndexes, "ORDINAL_POSITION", getInteger(tableColumn, tableColumnIndexes, "ORDINAL_POSITION"));
        updateValue(rowSet, columnIndexes, "IS_NULLABLE", getString(tableColumn, tableColumnIndexes, "IS_NULLABLE"));
        updateValue(rowSet, columnIndexes, "SPECIFIC_NAME",
                refCursor.specificName != null ? refCursor.specificName : refCursor.procedureName);

        rowSet.insertRow();
        rowSet.moveToCurrentRow();
    }

    private Integer getLength(ResultSet tableColumn, Map<String, Integer> tableColumnIndexes) throws SQLException {
        Integer charOctetLength = getInteger(tableColumn, tableColumnIndexes, "CHAR_OCTET_LENGTH");
        if (charOctetLength != null && charOctetLength > 0) {
            return charOctetLength;
        }
        return getInteger(tableColumn, tableColumnIndexes, "COLUMN_SIZE");
    }

    private Integer getInteger(ResultSet resultSet, Map<String, Integer> columnIndexes, String columnName) throws SQLException {
        Integer columnIndex = columnIndexes.get(columnName);
        if (columnIndex == null) {
            return null;
        }

        int value = resultSet.getInt(columnIndex);
        return resultSet.wasNull() ? null : value;
    }

    private String getString(ResultSet resultSet, Map<String, Integer> columnIndexes, String columnName) throws SQLException {
        Integer columnIndex = columnIndexes.get(columnName);
        if (columnIndex == null) {
            return null;
        }

        return resultSet.getString(columnIndex);
    }

    private void updateValue(CachedRowSet rowSet, Map<String, Integer> columnIndexes, String columnName, Object value)
            throws SQLException {
        Integer columnIndex = columnIndexes.get(columnName);
        if (columnIndex == null) {
            return;
        }

        if (value == null) {
            rowSet.updateNull(columnIndex);
        } else {
            rowSet.updateObject(columnIndex, value);
        }
    }

    private boolean isExplicitLookup(String objectNamePattern) {
        return objectNamePattern != null && !objectNamePattern.contains("%");
    }

    private String defaultSchema(String schema) {
        String normalized = normalizeIdentifier(schema);
        return normalized != null ? normalized : currentUserName;
    }

    private String normalizeIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }

        String trimmed = identifier.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() > 1) {
            return trimmed.substring(1, trimmed.length() - 1);
        }

        return trimmed.toUpperCase();
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static final class RefCursorDescriptor {
        private final String procedureCatalog;
        private final String procedureSchema;
        private final String procedureName;
        private final String columnName;
        private final String specificName;

        private RefCursorDescriptor(
                String procedureCatalog,
                String procedureSchema,
                String procedureName,
                String columnName,
                String specificName
        ) {
            this.procedureCatalog = procedureCatalog;
            this.procedureSchema = procedureSchema;
            this.procedureName = procedureName;
            this.columnName = columnName;
            this.specificName = specificName;
        }
    }

    private static final class TableReference {
        private final String schema;
        private final String table;

        private TableReference(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }
    }

    private static final class ColumnDefinition {
        private final String columnName;
        private final int dataType;
        private final String typeName;
        private final int columnSize;
        private final int decimalDigits;
        private final int nullable;
        private final int ordinalPosition;

        private ColumnDefinition(
                String columnName,
                int dataType,
                String typeName,
                int columnSize,
                int decimalDigits,
                int nullable,
                int ordinalPosition
        ) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
            this.nullable = nullable;
            this.ordinalPosition = ordinalPosition;
        }
    }
}
