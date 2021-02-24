package shared.messages;

import java.util.*;
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
 * <li>Metadata / Data: JSON objects 
 * </ul>
 * 
 */
public class KVAdminMessage {
    
    public enum KVAdminType {
        /* Undefined messages */
        UNDEFINED,      // Error: undefined type
        /* Type representing KVServer status */
        INIT,           // KVServer is created but not running
        START,          // KVServer is created and running
        UPDATE,         // KVServer needs to update metadata
        STOP,           // KVServer is stopped
        /* Data transfer betwen distributed KVServers */
		SEND        	// KVServer data transfer
    }

    public KVAdminType kvAdminType;
    public Map<String, Metadata> metadataList;  // servername, metadata

    /**
     * Construct KVAdminMessage with message type and metadata list.
     * @param status KVAdminMessage type.
     * @param list Metadata list.
     */
    public KVAdminMessage(KVAdminType type, Map<String, Metadata> list){
        this.kvAdminType = type;
        this.metadataList = list;
    }

    /**
     * Construct KVAdminMessage with JSON object string. 
     * @param jsonObjString 
     */
    public KVAdminMessage(String msgString){
        String[] tokens = msgString.split("/");
        this.kvAdminType = getMessageTypeEnum(tokens[0]);
        Gson gsonObj = new Gson();
        this.metadataList = gsonObj.fromJson(tokens[0], Map.class);
    }

    public KVAdminType getMessageType(){ return this.kvAdminType; }

    public Map<String, Metadata> getMetadataList(){ return this.metadataList; }

    public String getMessageTypeString(KVAdminType type){
        switch(type){
            case INIT: 
                return "INIT";
            case START:
                return "START";
            case UPDATE:
                return "UPDATE";
            case STOP:
                return "STOP";
            case SEND:
                return "SEND";
            default:
                return "UNDEFINED";
        }
    }

    public KVAdminType getMessageTypeEnum(String type){
        switch(type){
            case "INIT": 
                return KVAdminType.INIT;
            case "START":
                return KVAdminType.START;
            case "UPDATE":
                return KVAdminType.UPDATE;
            case "STOP":
                return KVAdminType.STOP;
            case "SEND":
                return KVAdminType.SEND;
            default:
                return KVAdminType.UNDEFINED;
        }
    }

    public String toJsonObjString(){
        Gson gsonObj = new Gson();
        String metadataString = gsonObj.toJson(metadataList);
        String msgString = getMessageTypeString(kvAdminType) + "/" + metadataString;
        return msgString;
    }

}
