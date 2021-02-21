package app_kvServer;

import java.util.ArrayList;
import java.util.Map;

import shared.messages.Metadata;

public interface IKVServer {

    /******************** Milestone 1 ********************/

    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Delete the value associated with the key
     * @return  if the delete operation is successful or not
     * @throws Exception
     *      when key not in the key range of the server
     */
    public boolean deleteKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

    /******************** Milestone 2 ********************/

    /**
     * Get metadata (information of other servers) for distributed servers. 
     */
    public ArrayList<Metadata> getMetaData();

    /**
     * Get the KV pairs that are stored but not responsible by the KVServer.
     * NOTE: this function is used to determine which part of data need to be transferred.
     * 
     * @return map of out-of-ranged KV pairs
     */
    public Map<String, String> getKVOutOfRange();

    /** 
     * Get write lock status of KVServer. 
     * 
     * @return writeLock representing if the server is writable.
     */
    public boolean getWriteLock();
    
    /** 
     * Aquires write lock of KVServer. 
     */
    public void aquireWriteLock();

    /** 
     * Release write lock of KVServer. 
     */
    public void releaseWriteLock();
}
