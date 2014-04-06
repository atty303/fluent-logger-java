//
// A Structured Logger for Fluent
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package org.fluentd.logger.sender;

import org.fluentd.logger.ErrorHandler;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public abstract class AbstractSender implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSender.class);

    private MessagePack msgpack;

    private ByteBuffer pendings;

    private Reconnector reconnector;

    private ErrorHandler errorHandler;

    public AbstractSender(int bufferCapacity, Reconnector reconnector) {
        msgpack = new MessagePack();
        msgpack.register(Event.class, Event.EventTemplate.INSTANCE);
        pendings = ByteBuffer.allocate(bufferCapacity);
        this.reconnector = reconnector == null ? new ExponentialDelayReconnector() : reconnector;
    }

    public void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    private boolean open(Event event) {
        try {
            connect();
            return true;
        } catch (IOException e) {
            LOG.error("Failed to connect fluentd: " + getDestinationName(), e);
            LOG.warn("Connection will be retried");
            try {
                if (errorHandler != null) {
                    errorHandler.onConnectionException(event, e);
                }
            } catch (Exception e1) {
                LOG.warn("Error handling failed (connection)", e1);
            } finally {
                close();
            }
            return false;
        }
    }

    protected abstract void connect() throws IOException;

    protected abstract boolean shouldConnect();

    protected abstract void write(byte[] buffer) throws IOException;

    protected abstract void disconnect();

    protected abstract String getDestinationName();

    private boolean reconnect(boolean forceReconnection, Event event) throws IOException {
        if (forceReconnection || shouldConnect()) {
            close();
            return open(event);
        }
        else {
            return true;
        }
    }

    public void close() {
        disconnect();
    }

    public boolean emit(String tag, Map<String, Object> data) {
        return emit(tag, System.currentTimeMillis() / 1000, data);
    }

    public boolean emit(String tag, long timestamp, Map<String, Object> data) {
        return emit(new Event(tag, timestamp, data));
    }

    protected boolean emit(Event event) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Created %s", new Object[]{event}));
        }

        byte[] bytes = null;
        try {
            // serialize tag, timestamp and data
            bytes = msgpack.write(event);
        } catch (IOException e) {
            LOG.error("Cannot serialize event: " + event, e);
            try {
                if (errorHandler != null) {
                    errorHandler.onSerializationException(event);
                }
            } catch (Exception e1){
                LOG.warn("Error handling failed (serialization)", e1);
            }
            return false;
        }

        // send serialized data
        return send(bytes, event);
    }

    private synchronized boolean send(byte[] bytes, Event event) {
        // buffering
        if (pendings.position() + bytes.length > pendings.capacity()) {
            LOG.error("Cannot send logs to " + getDestinationName());
            try {
                if (errorHandler != null) {
                    errorHandler.onBufferFullException(event);
                }
            } catch (Exception e1){
                LOG.warn("Error handling failed (buffer full)", e1);
            }
            return false;
        }
        pendings.put(bytes);

        // suppress reconnection burst
        if (!reconnector.enableReconnection(System.currentTimeMillis())) {
            return true;
        }

        // send pending data
        flush(event);

        return true;
    }

    public void flush() {
        flush(null);
    }

    protected synchronized void flush(Event eventNullable) {
        try {
            // check whether connection is established or not
            if (!reconnect(!reconnector.isErrorHistoryEmpty(), eventNullable)) {
                return;
            }
            write(getBuffer());
            clearBuffer();
            reconnector.clearErrorHistory();
        } catch (IOException e) {
            LOG.error(this.getClass().getName(), "flush", e);
            reconnector.addErrorHistory(System.currentTimeMillis());
            try {
                if (errorHandler != null) {
                    errorHandler.onSendingException(eventNullable, e);
                }
            } catch (Exception e1){
                LOG.warn("Error handling failed (sending)", e1);
            }
        }
    }

    public byte[] getBuffer() {
        int len = pendings.position();
        pendings.position(0);
        byte[] ret = new byte[len];
        pendings.get(ret, 0, len);
        return ret;
    }

    private void clearBuffer() {
        pendings.clear();
    }

    public abstract String getName();

    @Override
    public String toString() {
        return getName();
    }
}
