package org.fluentd.logger;

import org.fluentd.logger.sender.Event;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MockErrorHandler implements ErrorHandler {
    public volatile List<Tuple<Event, IOException>> connectionExceptions = new LinkedList();
    public volatile List<Tuple<Event, IOException>> sendingExceptions = new LinkedList();
    public volatile List<Event> bufferFullExceptions = new LinkedList();
    public volatile List<Event> serializationExceptions = new LinkedList();

    public static class Tuple<FST, SND> {
        public FST first;
        public SND second;
        public Tuple(FST f, SND s) {
            first = f;
            second = s;
        }
    }

    public void clearAll() {
        connectionExceptions.clear();
        sendingExceptions.clear();
        bufferFullExceptions.clear();
        serializationExceptions.clear();
    }

    @Override
    public void onConnectionException(Event eventNullable, IOException e) {
        connectionExceptions.add(new Tuple(eventNullable, e));
    }

    @Override
    public void onSendingException(Event eventNullable, IOException e) {
        sendingExceptions.add(new Tuple(eventNullable, e));
    }

    @Override
    public void onBufferFullException(Event lastEvent) {
        bufferFullExceptions.add(lastEvent);
    }

    @Override
    public void onSerializationException(Event event) {
        serializationExceptions.add(event);
    }
}
