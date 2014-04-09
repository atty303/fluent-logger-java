package org.fluentd.logger;

import org.fluentd.logger.sender.Event;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MockErrorHandler implements ErrorHandler {
    public volatile List<Tuple<Event, IOException>> connectionExceptions = new LinkedList();
    public volatile List<Tuple<Event, IOException>> sendingExceptions = new LinkedList();
    public volatile List<Tuple<Event, IOException>> serializationExceptions = new LinkedList();
    public volatile List<Event> bufferFulls = new LinkedList();

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
        serializationExceptions.clear();
        bufferFulls.clear();
    }

    @Override
    public void onConnectionException(Event event, IOException exception) {
        connectionExceptions.add(new Tuple(event, exception));
    }

    @Override
    public void onSendingException(Event event, IOException exception) {
        sendingExceptions.add(new Tuple(event, exception));
    }

    @Override
    public void onSerializationException(Event event, IOException exception) {
        serializationExceptions.add(new Tuple(event, exception));
    }

    @Override
    public void onBufferFull(Event event) {
        bufferFulls.add(event);
    }
}
