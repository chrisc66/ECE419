package shared.communication;

import shared.messages.KVMessage;

import java.io.IOException;

public interface IKVCommunication {
    /**
     * Sends a KVMessage using socket.
     *
     * @param msg the message to be sent.
     * @throws IOException output stream I/O exception.
     */
    public void send(KVMessage msg) throws IOException;

    /**
     * Receives a KVMessage using socket.
     *
     * @return the message to be received.
     * @throws IOException input stream I/O exception.
     */
    public KVMessage recv() throws IOException;
}