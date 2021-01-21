package shared.messages;

import java.util.Arrays;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Represents a <code>KVMessage</code> instance used in server and client connection (KVConnection)
 */
public class KVMessageClass implements KVMessage, Serializable {

    private static Logger logger = Logger.getRootLogger();
    private static final String DELIMITOR = System.getProperty("line.separator");
    private static final int MAX_KEY_SIZE = 20;
    private static final int BUFFER_SIZE = 1024;
    private static final int MAX_BUFF_SIZE = 120 * BUFFER_SIZE;

    private byte[] messageBytes;        // message in byte array format
    private String messageString;       // message in string format
    private String statusTypeString;    // 1st element in message
    private String key;                 // 2nd element in message
    private String value;               // 3rd element in message

    /**
     * Constructing a new KVMessageClass.
     * 
     * @param msgBytes Message content in byte array format.
     */
    public KVMessageClass(byte[] msgBytes) throws Exception {
        this.messageBytes = Arrays.copyOf(msgBytes, msgBytes.length);
        this.messageString = Arrays.toString(messageBytes);
        String[] elements = messageString.split(DELIMITOR, 3); 
        this.statusTypeString = elements[0];
        this.key = elements[1];
        this.value = elements[2];
        // this.statusTypeString = extractElement(messageString, 1); // 1st element
        // this.key = extractElement(messageString, 2);              // 2nd element
        // this.value = extractElement(messageString, 3);            // 3rd element
        if (key.getBytes().length > MAX_KEY_SIZE){
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        
        if (messageBytes.length > MAX_BUFF_SIZE){
            throw new Exception("Message exceed maximum allowed size of " + MAX_BUFF_SIZE + "bytes.");
        }
    }

    /**
     * Constructing a new KVMessageClass.
     * 
     * @param msgString Message content in string format.
     */
    public KVMessageClass(String msgString) throws Exception {
        this.messageString = msgString;
        this.messageBytes = msgString.getBytes();
        String[] elements = messageString.split(DELIMITOR, 3); 
        this.statusTypeString = elements[0];
        this.key = elements[1];
        this.value = elements[2];
        // this.statusTypeString = extractElement(messageString, 1); // 1st element
        // this.key = extractElement(messageString, 2);              // 2nd element
        // this.value = extractElement(messageString, 3);            // 3rd element
        if (key.getBytes().length > MAX_KEY_SIZE){
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        
        if (messageBytes.length > MAX_BUFF_SIZE){
            throw new Exception("Message exceed maximum allowed size of " + MAX_BUFF_SIZE + "bytes.");
        }
    }

    /**
     * Constructing a new KVMessageClass.
     * 
     * @param statusType first element, message status type.
     * @param key second element, key string.
     * @param value third element, value string.
     */
    public KVMessageClass(StatusType statusType, String key, String value) throws Exception {
        this.messageString = statusTypeString + DELIMITOR + key + DELIMITOR + value;
        this.messageBytes = messageString.getBytes();
        this.statusTypeString = getStatusString(statusType);        // 1st element
        this.key = key;                                             // 2nd element
        this.value = value;                                         // 3rd element
        if (key.getBytes().length > MAX_KEY_SIZE){
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        
        if (messageBytes.length > MAX_BUFF_SIZE){
            throw new Exception("Message exceed maximum allowed size of " + MAX_BUFF_SIZE + "bytes.");
        }
    }

    /**
     * Constructing a new KVMessageClass.
     * 
     * @param statusType first element, message status type.
     * @param key second element, key string.
     * @param value third element, value string.
     */
    public KVMessageClass(String statusTypeString, String key, String value) throws Exception {
        this.messageString = statusTypeString + DELIMITOR + key + DELIMITOR + value;
        this.messageBytes = messageString.getBytes();
        this.statusTypeString = statusTypeString;                   // 1st element
        this.key = key;                                             // 2nd element
        this.value = value;                                         // 3rd element
        if (key.getBytes().length > MAX_KEY_SIZE){
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        
        if (messageBytes.length > MAX_BUFF_SIZE){
            throw new Exception("Message exceed maximum allowed size of " + MAX_BUFF_SIZE + "bytes.");
        }
    }

    public StatusType getStatus(){
        return getStatus(this.statusTypeString);
    }

    public StatusType getStatus(String statusTypeString){
        switch(statusTypeString){
            case "GET": 
                return StatusType.GET;
            case "GET_ERROR":
                return StatusType.GET_ERROR;
            case "GET_SUCCESS":
                return StatusType.GET_SUCCESS;
            case "PUT":
                return StatusType.PUT;
            case "PUT_SUCCESS":
                return StatusType.PUT_SUCCESS;
            case "PUT_UPDATE":
                return StatusType.PUT_UPDATE;
            case "PUT_ERROR":
                return StatusType.PUT_ERROR;
            case "DELETE_SUCCESS":
                return StatusType.DELETE_SUCCESS;
            case "DELETE_ERROR":
                return StatusType.DELETE_ERROR;
            default:
                return StatusType.UNDEFINED;
        }
    }

    public String getStatusString(){
        return this.statusTypeString;
    }

    public String getStatusString(StatusType statusType){
        switch(statusType){
            case GET: 
                return "GET";
            case GET_ERROR:
                return "GET_ERROR";
            case GET_SUCCESS:
                return "GET_SUCCESS";
            case PUT:
                return "PUT";
            case PUT_SUCCESS:
                return "PUT_SUCCESS";
            case PUT_UPDATE:
                return "PUT_UPDATE";
            case PUT_ERROR:
                return "PUT_ERROR";
            case DELETE_SUCCESS:
                return "DELETE_SUCCESS";
            case DELETE_ERROR:
                return "DELETE_ERROR";
            default:
                return "UNDEFINED";
        }
    }

    public String getKey(){
        return key;
    }

    public String getValue(){
        return value;
    }

    public byte[] getMessageBytes(){
        return messageBytes;
    }

    public String getMessage(){
        return Arrays.toString(messageBytes);
    }

    // public String extractElement(String msg, int targetElementIdx){
    //     char[] msgArray = msg.toCharArray();
    //     String targetString = "";
    //     int currentElementIdx = 1;
    //     for (int i = 0; i < msg.length(); i++){
    //         if (currentElementIdx == targetElementIdx){
    //             targetString = targetString + msgArray[i];
    //         }
    //         if (currentElementIdx > targetElementIdx){
    //             break;
    //         }
    //         if (Character.compare(msgArray[i], DELIMITOR) == 0){
    //             currentElementIdx++;
    //         }
    //     }
    //     return targetString;
    // }

}
