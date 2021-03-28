package shared.communication;

import shared.messages.KVMessage;

import java.io.IOException;


/**
 * This class represents the communication interface between client and server.
 * <p>
 * Structure of its subclasses:
 * <ul>
 * <li>KVCommunicationClient -> IKVCommunication</li>
 * <li>KVCommunicationServer -> Runnable + KVCommunicationClient -> IKVCommunication</li>
 * </ul>
 * </p>
 */
public interface IKVCommunication {

    /**
     * Returns the connection status of open or not.
     * 
     * @return boolean variable of connection status.
     */
    public boolean isOpen();

    /**
     * Sends a KVMessage using socket.
     *
     * @param message the message to be sent.
     * @throws IOException output stream I/O exception.
     */
    public void send(KVMessage message) throws IOException;

    /**
     * Receives a KVMessage using socket.
     * This method blocks until input data is available, the
     * complete message is received, or an exception is thrown.
     *
     * @return the message to be received.
     * @throws IOException input stream I/O exception.
     * @throws Exception illegal message content lenth.
     */
    public KVMessage receive() throws IOException, Exception;

    /**
     * Closes current connection.
     * 
     * @throws IOException input stream I/O exception.
     */
    public void close() throws IOException;

}

