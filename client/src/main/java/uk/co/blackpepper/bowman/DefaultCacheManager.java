package uk.co.blackpepper.bowman;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public enum DefaultCacheManager {
    INSTANCE;

    private final String tempDir = System.getProperty("java.io.tmpdir");
    private final String db = "jdbc:h2:" + tempDir + "/suppliersDatabaseCache";
    private Connection conn;
    private final String defaultTableName = "HTTP_CACHE";
    private final Logger logger = LoggerFactory.getLogger(DefaultCacheManager.class);

    private CacheCommands cacheCommands;


     DefaultCacheManager() {
    }

    private Connection getDefaultConnection() {
        if (conn == null) {
            try {
                String dbUser = "dbUser";
                String dbPassword = "secret";
                conn = DriverManager.getConnection(db, dbUser, dbPassword);
                DatabaseMetaData dbm = conn.getMetaData();
                ResultSet rs = dbm.getTables(null, null, defaultTableName, null);

                if (rs.next()) {
                    logger.info("Cache table already exist.");
                } else {
                    logger.info("Cache table does not exist. creating it now.");
                    Statement stmt = conn.createStatement();
                    String CreateTableQuery = "CREATE TABLE " + defaultTableName + " (URL CLOB NULL, ETAG CLOB NULL, RESPONSE_OBJECT BLOB NULL)";
                    stmt.execute(CreateTableQuery);
                }
            } catch (SQLException e) {
                logger.error("Error creating cache database: " + e.getMessage());
            }
        }
        return conn;
    }

    public CacheCommands setConnection(Connection newConn) {
        if(this.cacheCommands == null) {
            this.cacheCommands = new CacheCommands(newConn, defaultTableName);
        }
        return this.cacheCommands;
    }

    public CacheCommands useDefaultConnection() {
        if(this.cacheCommands == null) {
            this.cacheCommands = new CacheCommands(getDefaultConnection(), defaultTableName);
        }
        return this.cacheCommands;
    }

    public CacheCommands getCacheCommands(){
        return this.cacheCommands;
    }
}
