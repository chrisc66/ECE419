package ecs;

import client.KVCommInterface;
import shared.messages.KVMessage;

public class ECStoServerComm implements KVCommInterface {

    @Override
    public void connect() throws Exception {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        return null;
    }

    @Override
    public KVMessage get(String key) throws Exception {
        return null;
    }

}
