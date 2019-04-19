package uk.co.blackpepper.bowman;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

public enum CacheManager {
    INSTANCE;

    private String tempDir = System.getProperty("java.io.tmpdir");
    private String db = "jdbc:h2:"+tempDir+"/suppliersDatabaseCache";
    private String dbUser = "dbUser";
    private String dbPassword = "secret";
    private Connection conn;
    private final Logger logger = LoggerFactory.getLogger(CacheManager.class);

    private String tableName = "HTTP_CACHE";

    private String getCacheByUrlQuery = "SELECT RESPONSE_OBJECT FROM " + tableName + " WHERE URL=?";
    private String getEtagByUrlQuery = "SELECT ETAG FROM " + tableName + " WHERE URL=?";
    private String insertCacheByUrlQuery = "INSERT INTO " + tableName + " VALUES(?, ?, ?)";
    private String updateCacheByUrlQuery = "UPDATE " + tableName + " SET ETAG=?, RESPONSE_OBJECT=? WHERE URL=?";
    private String evictCache = "DELETE FROM " + tableName;

    private CacheManager() {
        try {
            conn = DriverManager.getConnection(db, dbUser, dbPassword);
            DatabaseMetaData dbm = conn.getMetaData();
            ResultSet rs = dbm.getTables(null, null, tableName, null);

            if (rs.next()) {
                logger.info("Cache table already exist.");
            } else {
                logger.info("Cache table does not exist. creating it now.");
                Statement stmt = conn.createStatement();
                String CreateTableQuery = "CREATE TABLE " + tableName + " (URL CLOB NULL, ETAG CLOB NULL, RESPONSE_OBJECT BLOB NULL)";
                stmt.execute(CreateTableQuery);
            }
        } catch (SQLException e) {
            logger.error("Error creating cache database: " + e.getMessage());
        }
    }

    public void writeCacheForUrl(String url, String etag, byte[] ResponseObject) {
        try {
            // if we have the url in cache then update it.
            if (getCacheForUrl(url) != null) {
                PreparedStatement preparedStmt = conn.prepareStatement(updateCacheByUrlQuery);
                preparedStmt.setString(1, etag);

                InputStream ResponseObjectStream = new ByteArrayInputStream(ResponseObject);
                preparedStmt.setBinaryStream(2, ResponseObjectStream);

                preparedStmt.setString(3, url);

                logger.info("updating object in cache for url: " + url);
                preparedStmt.executeUpdate();
            } else {
                // we do not have the url cached then insert it
                PreparedStatement preparedStmt = conn.prepareStatement(insertCacheByUrlQuery);

                preparedStmt.setString(1, url);
                preparedStmt.setString(2, etag);

                InputStream ResponseObjectStream = new ByteArrayInputStream(ResponseObject);
                preparedStmt.setBinaryStream(3, ResponseObjectStream);

                logger.info("inserting new object to cache for url: " + url);
                preparedStmt.execute();
            }
        } catch (SQLException ex) {
            logger.info("Cache read error " + ex.getMessage(), ex.getMessage());
        }
    }

    public byte[] getCacheForUrl(String url) {
        try {
            PreparedStatement getCacheByUrlStmt = conn.prepareStatement(getCacheByUrlQuery);
            getCacheByUrlStmt.setString(1, url);
            ResultSet rs = getCacheByUrlStmt.executeQuery();
            rs.first();
            logger.info("Returning object from cache for " + url);
            return IOUtils.toByteArray(rs.getBinaryStream("RESPONSE_OBJECT"));
        } catch (SQLException | IOException ex) {
            logger.error("Error reading object from cache or cache miss url " + url + " : " + ex.getMessage());
        }
        return null;
    }

    public String getEatgForUrl(String url) {
        try {
            PreparedStatement getCacheByUrlStmt = conn.prepareStatement(getEtagByUrlQuery);
            getCacheByUrlStmt.setString(1, url);
            ResultSet rs = getCacheByUrlStmt.executeQuery();
            rs.first();
            logger.info("Url exist in cache and has Etag " + url);
            return rs.getString("ETAG");
        } catch (SQLException ex) {
            logger.error("Error reading Etag for " + url + " : " + ex.getMessage());
        }
        return null;
    }

    public void evictCache() {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(evictCache);
            logger.info("Evicting all cache");
        } catch (SQLException ex) {
            logger.error("Error evicting all cache: " + ex.getMessage());
        }
    }

    public void evictCacheForUrl(String url) {
        try {
            PreparedStatement getCacheByUrlStmt = conn.prepareStatement(getEtagByUrlQuery);
            getCacheByUrlStmt.setString(1, url);
            getCacheByUrlStmt.executeQuery();
            logger.info("Evicting cache for url " + url);
        } catch (SQLException ex) {
            logger.error("Error evicting cache for url " + url + " : " + ex.getMessage());
        }
    }
}
