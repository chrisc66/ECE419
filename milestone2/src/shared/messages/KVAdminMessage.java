package shared.messages;

import java.util.*;
import org.json.*;
import com.google.gson.Gson;

public class KVAdminMessage {
    
    public enum KVAdminType {
        UNDEFINED,  // Error: undefined type
        INIT,       // KVServer is created but not running
        START,      // KVServer is created and running
        UPDATE,     // KVServer needs to update metadata
        STOP        // KVServer is stopped
    }

    /**
     * KVAdminMessage JSON format
     * [
     *   "KVAdminMessage",
     *   KVAdminType,
     *   {
     *     "servername1": Metadata1,
     *     "servername2": Metadata2
     *   }
     * ]
     */
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
