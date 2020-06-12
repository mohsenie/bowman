package uk.co.blackpepper.bowman;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

public class CacheCommands implements CacheCommandsInterface{

    private final Logger logger = LoggerFactory.getLogger(CacheCommands.class);
    private final Connection conn;
    private final String tableName;

    private final String getEtagByUrlQuery = "SELECT ETAG FROM  %s  WHERE URL=?";

    public CacheCommands(Connection newConnection, String tableName){
        conn = newConnection;
        this.tableName = tableName;
    }

    public void writeCacheForUrl(String url, String etag, byte[] responseObject) {
        try {
            // if we have the url in cache then update it.
            if (getCacheForUrl(url) != null) {
                String updateCacheByUrlQuery = "UPDATE %s SET ETAG=?, RESPONSE_OBJECT=? WHERE URL=?";
                PreparedStatement preparedStmt = conn.prepareStatement(String.format(updateCacheByUrlQuery, tableName));
                preparedStmt.setString(1, etag);

                InputStream responseObjectStream = new ByteArrayInputStream(responseObject);
                preparedStmt.setBinaryStream(2, responseObjectStream);

                preparedStmt.setString(3, url);

                logger.info("updating object in cache for url: " + url);
                preparedStmt.executeUpdate();
            } else {
                // we do not have the url cached then insert it
                String insertCacheByUrlQuery = "INSERT INTO  %s VALUES(?, ?, ?)";
                PreparedStatement preparedStmt = conn.prepareStatement(String.format(insertCacheByUrlQuery, tableName));

                preparedStmt.setString(1, url);
                preparedStmt.setString(2, etag);

                InputStream responseObjectStream = new ByteArrayInputStream(responseObject);
                preparedStmt.setBinaryStream(3, responseObjectStream);

                logger.info("inserting new object to cache for url: " + url);
                preparedStmt.execute();
            }
        } catch (SQLException ex) {
            logger.info("Cache read error " + ex.getMessage(), ex.getMessage());
        }
    }

    public byte[] getCacheForUrl(String url) {
        try {
            String getCacheByUrlQuery = "SELECT RESPONSE_OBJECT FROM %s WHERE URL=?";
            PreparedStatement getCacheByUrlStmt = conn.prepareStatement(String.format(getCacheByUrlQuery, tableName));
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
            PreparedStatement getCacheByUrlStmt = conn.prepareStatement(String.format(getEtagByUrlQuery, tableName));
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
            String evictCache = "DELETE FROM %s";
            stmt.execute(evictCache);
            logger.info("Evicting all cache");
        } catch (SQLException ex) {
            logger.error("Error evicting all cache: " + ex.getMessage());
        }
    }

    public void evictCacheForUrl(String url) {
        try {
            PreparedStatement getCacheByUrlStmt = conn.prepareStatement(String.format(getEtagByUrlQuery, tableName));
            getCacheByUrlStmt.setString(1, url);
            getCacheByUrlStmt.executeQuery();
            logger.info("Evicting cache for url " + url);
        } catch (SQLException ex) {
            logger.error("Error evicting cache for url " + url + " : " + ex.getMessage());
        }
    }
}
