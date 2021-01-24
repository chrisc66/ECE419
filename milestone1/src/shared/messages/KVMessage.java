package shared.messages;

/**
 * Represents a <code>KVMessage</code> used in server and client connection (KVConnection)
 * 
 * <p>
 * The message is sent through stream buffer (i.e. array of bytes), and follows
 * below protocol. 
 * <ul>
 * <li>StatusType: 1st element, status type represented by two digits within <code>00</code> - <code>99</code>) </li>
 * <li>DELIMITOR <code>System.getProperty("line.separator")</code> </li>
 * <li>Key: 2nd element, the former element in key-value pair, leave empty if none </li>
 * <li>DELIMITOR <code>System.getProperty("line.separator")</code> </li>
 * <li>Value: 3rd element, the latter element in key-value pair, leave empty if none</li>
 * <li>DELIMITOR <code>System.getProperty("line.separator")</code> </li>
 * </ul>
 * </p>
 */
public interface KVMessage {
	
	public enum StatusType {
		/* Undefined messages */
		UNDEFINED,		/* 00 Undefined - undefined status type, throw an exception */
		/* Standard KV messages */
		GET, 			/* 01 Get - request */
		GET_ERROR, 		/* 02 requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* 03 requested tuple (i.e. value) found */
		PUT, 			/* 04 Put - request */
		PUT_SUCCESS, 	/* 05 Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* 06 Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* 07 Put - request not successful */
		DELETE_SUCCESS, /* 08 Delete - request successful */
		DELETE_ERROR, 	/* 09 Delete - request successful */
		/* Disconnect message */
		DISCONNECT		/* 10 Disconnect - close connection */
	}

	/**
	 * Returns the status type of current KVMessage.
	 * 
	 * @return a statusType that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * Returns the status type of current KVMessage (constructor helper function).
	 * 
	 * @param statusTypeString a string that represents the StatusType of the message.
	 * @return a statusType that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus(String statusTypeString);

	/**
	 * Returns the status type string of current KVMessage.
	 * 
	 * @return a string that represents the StatusType of the message.
	 */
	public String getStatusString();

	/**
	 * Returns the status type string of current KVMessage (constructor helper function).
	 * 
	 * @param statusType a statusType that is used to identify message types.
	 * @return a string that represents the StatusType of the message.
	 */
	public String getStatusString(StatusType statusType);

	/**
	 * Returns the key of key-value pair in current KVMessage.
	 * 
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * Returns the value of key-value pair in current KVMessage.
	 * 
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes 
	 * 		in ASCII coding.
	 */
	public byte[] getMessageBytes();

	/**
	 * Returns the content of this TextMessage as a String.
	 * 
	 * @return the content of this message in String format.
	 */
	public String getMessage();

	// /**
    //  * Parse the message string and obtain targeted element. 
    //  * 
    //  * @param msg Message string to be parsed. 
    //  * @param targetElementIdx Index of targetted element to be parsed.
	//  * <ol>
	//  * <li>StatusType: 1st element, status type represented by two digits within <code>00</code> - <code>99</code>) </li>
	//  * <li>Key: 2nd element, the former element in key-value pair, leave empty if none </li>
	//  * <li>Value: 3rd element, the latter element in key-value pair, leave empty if none</li>
	//  * </ol>
    //  * @return String of returned element
    //  */
	// public String extractElement(String msg, int targetElementIdx);
}


