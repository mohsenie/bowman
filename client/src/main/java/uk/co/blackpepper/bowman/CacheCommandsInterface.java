package uk.co.blackpepper.bowman;

public interface CacheCommandsInterface {
    void writeCacheForUrl(String url, String etag, byte[] ResponseObject);
    byte[] getCacheForUrl(String url);
    String getEatgForUrl(String url);
    void evictCache();
    void evictCacheForUrl(String url);
}
