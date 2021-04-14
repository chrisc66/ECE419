package DiskStorage;

import org.apache.log4j.Logger;
import java.util.*;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DiskStorage implements DiskStorageInterface{

    /**
     * Input: This should have a list of KV pairs as inputs or single KV pairs
     * Ouput: return value or error messages for get requests
     * **/

    private static Logger logger = Logger.getRootLogger();
    private Map<String, String> LookUpTable;

    private String dir = "./data";
    private String fileName;
    private File storageFile;

    public DiskStorage(String serverName){
        this.fileName = "persistanceDB.properties"+"."+serverName;
        initalizeFile();
        Map<String, String>new_map = new HashMap<String, String>();
        this.LookUpTable = Collections.synchronizedMap(new_map);
    }

    public DiskStorage(String filePrefix, String serverName){
        this.fileName = filePrefix+"."+serverName;
        initalizeFile();
        this.LookUpTable = Collections.synchronizedMap(loadHashMapFromFile());
    }

    /**
     * Reading the file using Java IO
     */
    private synchronized void initalizeFile(){
        if (this.storageFile==null){
            //first make the data directory
            logger.info("Initializing database file ...");
            File dir = new File(this.dir);
            boolean dataFileNotExist;
            if (!dir.exists()){
                try {
                    dir.mkdir();
                } catch (Exception error){
                    logger.error("Unable to initialize database directory\n", error);
                }
            }
            this.storageFile = new File(this.dir+'/'+this.fileName);
            try {
                dataFileNotExist = this.storageFile.createNewFile();
                if (dataFileNotExist) {
                    logger.info("New data file created");
                } else {
                    logger.info("Data Storage file found");
                }
            } catch (IOException e) {
                logger.error("Error initializing file instance", e);
            }
        }
    }

    private synchronized Map<String,String>loadHashMapFromFile(){
        Map<String, String> lookUpTableContent = new HashMap<String, String>();
        Properties properties = new Properties();
        try{
            properties.load(new FileInputStream(this.dir+'/'+this.fileName));
            for (String key : properties.stringPropertyNames()) {
                lookUpTableContent.put(key, properties.get(key).toString());
            }
        }catch (IOException error){
         logger.error("Error loading Hashmap from File");
        }

        return lookUpTableContent;
    }

    private synchronized void storeMapDataIntoFile(){
        Properties properties = new Properties();

        for (Map.Entry<String,String> entry : this.LookUpTable.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        try{
            properties.store(new FileOutputStream(this.dir+'/'+this.fileName), null);
        }catch (IOException e){
            logger.error("Error saving file content");
        }

    }

    @Override
    public synchronized boolean put(String key, String val){
        //update the value
        try{
            this.LookUpTable.put(key,val);
            logger.info("Successfully Inserted KV pair of: " + key+'-'+val);
            storeMapDataIntoFile();
            return true;
        }catch (Exception e){
            logger.error("Error trying to insert key value pair of: "+ key+'-'+val,e);
            return false;
        }

    }
    @Override
    public synchronized String get(String key){
        try{
            String val = this.LookUpTable.get(key);
            if (val==null){
                logger.info("Did not find the corresponding value for the given key of "+key);
            }else {
                logger.info("Successfully get KV pair of: " + key + '-' + val);
            }
            return val;
        }catch (Exception e){
            logger.error("Error trying to get value of key: "+ key,e);
            return null;
        }
    }

    @Override
    public synchronized void clearDisk() {
        try{
            this.LookUpTable.clear();
            storeMapDataIntoFile();
        }catch (Exception e){
            logger.error("Error trying to clear the map",e);
        }
    }

    @Override
    public synchronized boolean delelteKV(String key) {
        try {
            String val =this.LookUpTable.remove(key);
            if (val == null){
                logger.info("Cannot remove non-exisitng KV pairs");
                return false;
            }
            storeMapDataIntoFile();
            return true;
        }catch (Exception e){
            logger.error("Error trying to remove entry of key: "+key);
        }

        return false;
    }

    @Override
    public synchronized boolean onDisk(String key) {
        if(this.LookUpTable.isEmpty()){
            return false;
        }
        return this.LookUpTable.containsKey(key);
    }

    public Map<String, String> getKVOutOfRange(BigInteger start, BigInteger stop){
		Map<String, String> KVtable = Collections.synchronizedMap(loadHashMapFromFile());
        Map<String, String> KVOutOfRange = new HashMap<String, String>();
        for (Map.Entry<String, String> kv : KVtable.entrySet()) { 
            String key = (String) kv.getKey(); 
            if (!keyWithinRange(mdKey(key), start, stop)){
                String value = (String) kv.getValue(); 
                KVOutOfRange.put(key, value);
            }
        }
        return KVOutOfRange;
	}

    public Map<String, String> getKVWithinRange(BigInteger start, BigInteger stop){
		Map<String, String> KVtable = Collections.synchronizedMap(loadHashMapFromFile());
        Map<String, String> KVWithinRange = new HashMap<String, String>();
        for (Map.Entry<String, String> kv : KVtable.entrySet()) { 
            String key = (String) kv.getKey(); 
            if (keyWithinRange(mdKey(key), start, stop)){
                String value = (String) kv.getValue(); 
                KVWithinRange.put(key, value);
            }
        }
        return KVWithinRange;
	}

    public Map<String, String> getAllKV(){
		return Collections.synchronizedMap(loadHashMapFromFile());
	}

    public boolean keyWithinRange (BigInteger mdKey, BigInteger start, BigInteger stop){
        // mdKey >= start -> 0 or 1
        // mdKey < stop -> -1
        // START <= STOP && key > START && key < STOP
        // START >= STOP && key > START && key > STOP
        // START >= STOP && key < START && key < STOP
        if ((start.compareTo(stop) !=  1) && (mdKey.compareTo(start) ==  1 && mdKey.compareTo(stop) == -1) || 
            (start.compareTo(stop) != -1) && (mdKey.compareTo(start) ==  1 && mdKey.compareTo(stop) ==  1) || 
            (start.compareTo(stop) != -1) && (mdKey.compareTo(start) == -1 && mdKey.compareTo(stop) == -1) )
            return true;
        return false;
    }

    /**
	 * helper function for getting MD5 hash key
	 * may need to move to some shared class for being visible for both client and server
	 */
	public BigInteger mdKey (String key) {
		MessageDigest md = null;
        try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger.error("NoSuchAlgorithmException occured!");
		}
        byte[] md_key = md.digest(key.getBytes());
        BigInteger md_key_bi = new BigInteger(1, md_key);
		return md_key_bi; 
	}
}
