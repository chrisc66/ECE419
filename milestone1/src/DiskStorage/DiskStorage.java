package DiskStorage;

import org.apache.log4j.Logger;
import DiskStorage.KeyValuePair;
import java.util.*;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

public class DiskStorage implements DiskStorageInterface{

    /**
     * Input: This should have a list of KV pairs as inputs or single KV pairs
     * Ouput: return value or error messages for get requests
     * **/

    private static Logger logger = Logger.getRootLogger();
    private Map<String, String> LookUpTable;

    private String dir = "./data";
    private String fileName = "persistanceDB.properties";
    private File storageFile;


    public DiskStorage(){
        initalizeFile();
        Map<String, String>new_map = new HashMap<String, String>();
        this.LookUpTable = Collections.synchronizedMap(new_map);
    }

    public DiskStorage(String fileName){
        this.fileName = fileName;
        initalizeFile();
        this.LookUpTable = Collections.synchronizedMap(loadHashMapFromFile());
    }


    /**Reading the file using Java IO*/
    private void initalizeFile(){
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

    private Map<String,String>loadHashMapFromFile(){
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

    private void storeMapDataIntoFile(){
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
    public boolean put(String key, String val){
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
    public String get(String key){
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
    public void clearDisk() {
        try{
            this.LookUpTable.clear();
            storeMapDataIntoFile();
        }catch (Exception e){
            logger.error("Error trying to clear the map",e);
        }
    }

    @Override
    public boolean delelteKV(String key) {
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
    public boolean onDisk(String key) {
        if(this.LookUpTable.isEmpty()){
            return false;
        }
        return this.LookUpTable.containsKey(key);
    }


}
