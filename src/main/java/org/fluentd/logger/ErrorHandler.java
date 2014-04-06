package org.fluentd.logger;

import org.fluentd.logger.sender.Event;

import java.io.IOException;

public interface ErrorHandler {
    void onConnectionException(Event e, IOException eventNullable);

    void onSendingException(Event e, IOException eventNullable);

    void onBufferFullException(Event lastEvent);

    void onSerializationException(Event event);
}
