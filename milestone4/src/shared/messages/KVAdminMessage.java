package shared.messages;

import java.util.*;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Type;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * KVAdminMessage is used to transfer admin messages between ECS and distributed KVServers.
 * 
 * <p>
 * The message is sent through ZooKeeper API (setData and getData) in the format of String transferred to byte array.
 * </p>
 * 
 * <ul>
 * <li>Source Server: source of KVServer sending this message </li>
 * <li>KVAdminType: represented by String </li>
 * <li>Metadata: Map of servername and metadata in JSON format, or null </li>
 * <li>KV Data: Map of key and value represented in JSON format, or null</li>
 * </ul>
 * 
 */
public class KVAdminMessage {
    
    public enum KVAdminType {
        /* Undefined messages */
        UNDEFINED,          // Error: undefined / uninitialized type
        /* Type representing KVServer status */
        START,              // KVServer is created, respond to both ECS and KVClient
        UPDATE,             // KVServer needs to update metadata
        UPDATE_REMOVE,      // KVServer needs to update metadata when removing a node
        STOP,               // KVServer is stopped, only respond to ECS
        SHUTDOWN,           // KVServer is stopped, respond to neither ECS nor KVClient 
        /* Data transfer betwen distributed KVServers */
		TRANSFER_KV,       	// KVServer data transfer
        ACK_TRANSFER        // KVServer acknowledgement to data transfer
    }

    private final static String SEPARATOR = "/";
    private String messageSource;
    private KVAdminType messageType;
    private Map<String, Metadata> messageMetadata;  // servername, metadata
    private Map<String, String> messageKVData;      // key, value

    /**
     * Construct KVAdminMessage with message type, metadata and KV data.
     * @param src source of this message, KVServer or ECS.
     * @param type KVAdminMessage type.
     * @param metadata KVServer metadata (ECS hash ring).
     * @param data KV pairs data.
     */
    public KVAdminMessage(String src, KVAdminType type, Map<String, Metadata> metadata, Map<String, String> data){
        this.messageSource = src;
        this.messageType = type;
        this.messageMetadata = metadata;
        this.messageKVData = data;
    }

    /**
     * Construct KVAdminMessage with JSON object string. 
     * @param jsonObjString 
     */
    public KVAdminMessage(String msgString){
        String[] tokens = msgString.split(SEPARATOR);
        this.messageSource = tokens[0];
        this.messageType = getMessageType(tokens[1]);
        Gson gsonObj = new Gson();
        Type metadataType = new TypeToken<Map<String, Metadata>>(){}.getType();
        Type kvDataType = new TypeToken<Map<String, String>>(){}.getType();
        this.messageMetadata = gsonObj.fromJson(tokens[2], metadataType);
        this.messageKVData = gsonObj.fromJson(tokens[3], kvDataType);
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
            case START:
                return "START";
            case UPDATE:
                return "UPDATE";
            case UPDATE_REMOVE:
                return "UPDATE_REMOVE";
            case STOP:
                return "STOP";
            case SHUTDOWN:
                return "SHUTDOWN";
            case TRANSFER_KV:
                return "TRANSFER_KV";
            case ACK_TRANSFER:
                return "ACK_TRANSFER";
            default:
                return "UNDEFINED";
        }
    }

    public KVAdminType getMessageType(String type){
        switch(type){
            case "START":
                return KVAdminType.START;
            case "UPDATE":
                return KVAdminType.UPDATE;
            case "UPDATE_REMOVE":
                return KVAdminType.UPDATE_REMOVE;
            case "STOP":
                return KVAdminType.STOP;
            case "SHUTDOWN":
                return KVAdminType.SHUTDOWN;
            case "TRANSFER_KV":
                return KVAdminType.TRANSFER_KV;
            case "ACK_TRANSFER":
                return KVAdminType.ACK_TRANSFER;
            default:
                return KVAdminType.UNDEFINED;
        }
    }

    /**
     * Get the source of KVAdminMessage
     * @return return message source
     */
    public String getMessageSource(){
        return this.messageSource;
    }

    /**
     * Check whether the message is sent from ECS or another KVServer. 
     * @return boolean indicating whether from ECS or not. 
     */
    public boolean fromECS(){
        if (messageSource.equals("ECS"))
            return true;
        else
            return false;
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
        String msgString = messageSource + SEPARATOR + getMessageTypeString() + SEPARATOR + metadataStr + SEPARATOR + kvDataStr;
        return msgString;
    }

    public byte[] toBytes(){
        String msgString = this.toString();
        return msgString.getBytes(StandardCharsets.UTF_8);
    }

}
