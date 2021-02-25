package shared.messages;

import java.util.*;
import java.nio.charset.StandardCharsets;

import org.json.*;
import com.google.gson.Gson;

/**
 * KVAdminMessage is used to transfer admin messages between ECS and distributed KVServers.
 * 
 * <p>
 * The message is sent through ZooKeeper API (setData and getData) in the format of String transferred to byte array.
 * </p>
 * 
 * <ul>
 * <li>KVAdminType: represented by String </li>
 * <li>Metadata: Map of servername and metadata in JSON format, or null </li>
 * <li>KV Data: Map of key and value represented in JSON format, or null</li>
 * </ul>
 * 
 */
public class KVAdminMessage {
    
    public enum KVAdminType {
        /* Undefined messages */
        UNDEFINED,          // Error: undefined type
        /* Type representing KVServer status */
        INIT,               // KVServer is created, only respond to ECS
        START,              // KVServer is created, respond to both ECS and KVClient
        UPDATE,             // KVServer needs to update metadata
        STOP,               // KVServer is stopped, only respond to ECS
        SHUTDOWN,           // KVServer is stopped, respond to neither ECS nor KVClient 
        /* Data transfer betwen distributed KVServers */
		TRANSFER_KV        	// KVServer data transfer
    }

    private final static String separator = "/";
    private KVAdminType messageType;
    private Map<String, Metadata> messageMetadata;  // servername, metadata
    private Map<String, String> messageKVData;      // key, value

    /**
     * Construct KVAdminMessage with message type, metadata and KV data.
     * @param status KVAdminMessage type.
     * @param metadata KVServer metadata.
     * @param data KV pairs data.
     */
    public KVAdminMessage(KVAdminType type, Map<String, Metadata> metadata, Map<String, String> data){
        this.messageType = type;
        this.messageMetadata = metadata;
        this.messageKVData = data;
    }

    /**
     * Construct KVAdminMessage with JSON object string. 
     * @param jsonObjString 
     */
    public KVAdminMessage(String msgString){
        String[] tokens = msgString.split(separator);
        this.messageType = getMessageType(tokens[0]);
        Gson gsonObj = new Gson();
        this.messageMetadata = gsonObj.fromJson(tokens[1], Map.class);
        this.messageKVData = gsonObj.fromJson(tokens[2], Map.class);
    }

    /**
     * Get the type of KVAdminMessage
     * @return KVAdminType type of KVAdminMessage
     */
    public KVAdminType getMessageType(){ 
        return this.messageType; 
    }

    public String getMessageTypeString(){
        switch(messageType){
            case INIT: 
                return "INIT";
            case START:
                return "START";
            case UPDATE:
                return "UPDATE";
            case STOP:
                return "STOP";
            case SHUTDOWN:
                return "SHUTDOWN";
            case TRANSFER_KV:
                return "TRANSFER_KV";
            default:
                return "UNDEFINED";
        }
    }

    public KVAdminType getMessageType(String type){
        switch(type){
            case "INIT": 
                return KVAdminType.INIT;
            case "START":
                return KVAdminType.START;
            case "UPDATE":
                return KVAdminType.UPDATE;
            case "STOP":
                return KVAdminType.STOP;
            case "SHUTDOWN":
                return KVAdminType.SHUTDOWN;
            case "TRANSFER_KV":
                return KVAdminType.TRANSFER_KV;
            default:
                return KVAdminType.UNDEFINED;
        }
    }

    /**
     * Get the metadata map of KVAdminMessage
     * @return return metadata map
     */
    public Map<String, Metadata> getMessageMetadata(){ 
        return this.messageMetadata; 
    }
    
    /**
     * Get the KV pairs of KVAdminMessage
     * @return return KV pair data
     */
    public Map<String, String> getMessageKVData(){ 
        return this.messageKVData; 
    }

    public String toString(){
        Gson gsonObj = new Gson();
        String metadataStr = gsonObj.toJson(messageMetadata);
        String kvDataStr = gsonObj.toJson(messageKVData);
        String msgString = getMessageTypeString() + separator + metadataStr + separator + kvDataStr;
        return msgString;
    }

    public byte[] toBytes(){
        String msgString = this.toString();
        return msgString.getBytes(StandardCharsets.UTF_8);
    }

}
