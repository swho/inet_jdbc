# An Oracle JDBC Proxy Driver for I-net clear Report Designer

本專案是一個專為 **i-net report designer** 開發的 JDBC Proxy 驅動程式。

### 🚀 解決的核心問題
在使用 i-net report designer 連接大型 Oracle 資料庫時，設計工具往往會卡在 **"Reading database structure"** 階段長達數十秒甚至數分鐘。這是因為設計工具在連線初期會試圖抓取全庫數萬張資料表 (Tables) 與預存程序 (Procedures) 的中繼資料。

本代理驅動程式透過**攔截並限縮查詢範圍**至僅限當前登入的使用者，將原本漫長的讀取時間降至**毫秒級別**，大幅提升報表開發體驗。

## 核心功能
- 動態攔截並代理 `Connection.getMetaData()`。
- 對於耗時的 `getTables`, `getColumns`, `getProcedures` 等探索分析方法，強制將查詢範圍縮小至當前登入使用者 (Schema)，瞬間提升擷取速度。
- 完全保留 `Connection.prepareStatement(sql)` 以及 `ResultSetMetaData` 的行為，即使過濾了全域表的擷取，**撰寫 SQL 進行欄位抓取與測試時依然保持 100% 正確**。

## 安裝與編譯
這個專案依賴 Java 與 Maven，如果您的終端或者 IDE (例如 IntelliJ IDEA) 有安裝 Maven，請執行下面指令打出包含所有依賴的超級 Jar (Fat Jar)：

```bash
.\mvnw.cmd clean package   # Windows
# 或
./mvnw clean package       # Mac/Linux
```

產生的成品會放在 `target/` 目錄中，檔名為：
👉 `oracle-jdbc-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar`

---

## i-net Designer 設定教學

### 1. 部署驅動程式 (Driver Deployment)
將編譯好的 `oracle-jdbc-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar` 複製到 i-net Designer 的驅動程式目錄中：
- 預設安裝路徑通常為：`C:\Program Files\i-net Clear Reports Designer\lib\driver\`
- **重要提示**：請確保該目錄下也存在原生的 Oracle JDBC 驅動（例如 `ojdbc8.jar`）。

### 2. 在 Designer 中建立連線
1. 開啟 **i-net Clear Reports Designer**。
2. 點擊工具列的 **Database** -> **DriverManager**。
3. 在左側清單中點選 **user-defined driver ..**（或者點選右上角的 `+` 新增）。
4. **Driver 設定參數**：
   - **Driver Name**: `Oracle Proxy (swho)` (可自訂名稱)
   - **Driver Class**: `swho.jdbc.proxy.OracleProxyDriver`
   - **URL Prefix**: `jdbc:swhoproxy:`
5. **連線設定 (Connection Info)**：
   - 在 URL 欄位輸入帶有前綴的位址：
     `jdbc:swhoproxy:oracle:thin:@<HOST_IP>:<PORT>/<SERVICE_NAME>`
   - 輸入您的資料庫 **Username** 與 **Password**。
<img width="797" height="782" alt="image" src="https://github.com/user-attachments/assets/8b63b568-b840-4616-89a7-64b8615b0361" />


### 3. 驗證效能
點擊 **Test Connection**。若設定正確，您會發現連線速度極快，因為 Proxy 已經自動將中繼資料查詢範圍限縮在您的登入 Schema 內。

---

## 常見問題 (Q&A)

**Q: 為什麼我在左側樹狀清單看不到別人的 Table？**
A: 這是 Proxy 的核心優化機制。為了防止連線時卡頓，我們強制將搜尋範圍限定在您「登入的帳號」名下。若需查詢其他 Schema 的表，請直接在 SQL 編輯區手寫 `SELECT * FROM OTHER_SCHEMA.TABLE_NAME`。

**Q: 執行 SQL 會變慢嗎？**
A: 完全不會。本 Proxy 只攔截連線時的「結構探勘 (Metadata)」行為，一旦進入 SQL 執行階段，所有指令都是直接交由底層封裝的原生 Oracle 驅動程式處理。
