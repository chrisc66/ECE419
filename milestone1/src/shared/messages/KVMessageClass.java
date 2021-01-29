package shared.messages;

import java.util.Arrays;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Represents a <code>KVMessage</code> instance used in server and client connection (KVConnection)
 */
public class KVMessageClass implements KVMessage, Serializable {

    private static Logger logger = Logger.getRootLogger();
    private static final String DELIMITOR = "D" + "\n"; // "D" = 68, "\n" = 10
    private static final int MAX_KEY_SIZE = 20;
    private static final int MAX_VALUE_SIZE = 120 * 1024;

    private byte[] messageBytes;        // message in byte array format
    private String messageString;       // message in string format
    private String statusTypeString;    // 1st element in message
    private String key;                 // 2nd element in message
    private String value;               // 3rd element in message

    /**
     * Constructor 1: Constructing a new KVMessageClass.
     * 
     * @param msgBytes Message content in byte array format.
     */
    public KVMessageClass(byte[] msgBytes) throws Exception {

        this.messageBytes = new byte[msgBytes.length];
        System.arraycopy(msgBytes, 0, messageBytes, 0, msgBytes.length);
        
        this.messageString = new String(msgBytes, StandardCharsets.UTF_8);
        String[] elements = messageString.split(DELIMITOR, 4); 
        this.statusTypeString = elements[0];
        this.key = elements[1];
        this.value = elements[2];

        logger.debug("KVmessage constructor 1, key: " + key + ", value: " + value);

        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_SIZE){
            logger.error("Key length exceeds maximum allowed size of " + MAX_KEY_SIZE);
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_VALUE_SIZE){
            logger.error("Value length exceeds maximum allowed size of " + MAX_VALUE_SIZE);
            throw new Exception("Value exceed maximum allowed size of " + MAX_VALUE_SIZE + "bytes.");
        }

    }

    /**
     * Constructor 2: Constructing a new KVMessageClass.
     * 
     * @param msgString Message content in string format.
     */
    public KVMessageClass(String msgString) throws Exception {

        this.messageString = msgString;
        this.messageBytes = msgString.getBytes(StandardCharsets.UTF_8);
        
        String[] elements = messageString.split(DELIMITOR, 0); 
        this.statusTypeString = elements[0];
        this.key = elements[1];
        this.value = elements[2];

        logger.debug("KVmessage constructor 2, key: " + key + ", value: " + value);

        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_SIZE){
            logger.error("Key length exceeds maximum allowed size of " + MAX_KEY_SIZE);
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_VALUE_SIZE){
            logger.error("Value length exceeds maximum allowed size of " + MAX_VALUE_SIZE);
            throw new Exception("Value exceed maximum allowed size of " + MAX_VALUE_SIZE + "bytes.");
        }

    }

    /**
     * Constructor 3: Constructing a new KVMessageClass.
     * 
     * @param statusType first element, message status type.
     * @param key second element, key string.
     * @param value third element, value string.
     */
    public KVMessageClass(StatusType statusType, String key, String value) throws Exception {

        this.statusTypeString = getStatusString(statusType);        // 1st element
        this.key = key;                                             // 2nd element
        this.value = value;                                         // 3rd element

        this.messageString = statusTypeString + DELIMITOR + key + DELIMITOR + value + DELIMITOR;
        this.messageBytes = messageString.getBytes(StandardCharsets.UTF_8);

        logger.debug("KVmessage constructor 3, key: " + key + ", value: " + value);

        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_SIZE){
            logger.error("Key length exceeds maximum allowed size of " + MAX_KEY_SIZE);
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_VALUE_SIZE){
            logger.error("Value length exceeds maximum allowed size of " + MAX_VALUE_SIZE);
            throw new Exception("Value exceed maximum allowed size of " + MAX_VALUE_SIZE + "bytes.");
        }

    }

    /**
     * Constructor 4: Constructing a new KVMessageClass.
     * 
     * @param statusType first element, message status type string.
     * @param key second element, key string.
     * @param value third element, value string.
     */
    public KVMessageClass(String statusTypeString, String key, String value) throws Exception {
        
        this.statusTypeString = statusTypeString;                   // 1st element
        this.key = key;                                             // 2nd element
        this.value = value;                                         // 3rd element

        this.messageString = statusTypeString + DELIMITOR + key + DELIMITOR + value + DELIMITOR;
        this.messageBytes = messageString.getBytes(StandardCharsets.UTF_8);

        logger.debug("KVmessage constructor 4, key: " + key + ", value: " + value);

        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_SIZE){
            logger.error("Key length exceeds maximum allowed size of " + MAX_KEY_SIZE);
            throw new Exception("Key exceed maximum allowed size of " + MAX_KEY_SIZE + "bytes.");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_VALUE_SIZE){
            logger.error("Value length exceeds maximum allowed size of " + MAX_VALUE_SIZE);
            throw new Exception("Value exceed maximum allowed size of " + MAX_VALUE_SIZE + "bytes.");
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
            case "DISCONNECT":
                return StatusType.DISCONNECT;
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
            case DISCONNECT:
                return "DISCONNECT";
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

}
