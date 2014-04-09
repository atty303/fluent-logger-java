package org.fluentd.logger;

import org.fluentd.logger.sender.Event;

import java.io.IOException;

public interface ErrorHandler {
    void onConnectionException(Event event, IOException exception);

    void onSendingException(Event event, IOException exception);

    void onSerializationException(Event event, IOException exception);
    
    void onBufferFull(Event event);
}
