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

import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public class RawSocketSender extends AbstractSender implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(RawSocketSender.class);

    private SocketAddress server;

    private Socket socket;

    private int timeout;

    private BufferedOutputStream out;

    private String name;

    public RawSocketSender() {
        this("localhost", 24224);
    }

    public RawSocketSender(String host, int port) {
        this(host, port, 3 * 1000, 8 * 1024 * 1024);
    }

    public RawSocketSender(String host, int port, int timeout, int bufferCapacity) {
        this(host, port, timeout, bufferCapacity, null);

    }

    public RawSocketSender(String host, int port, int timeout, int bufferCapacity, Reconnector reconnector) {
        super(bufferCapacity, reconnector);
        server = new InetSocketAddress(host, port);
        name = String.format("%s_%d_%d_%d", host, port, timeout, bufferCapacity);
        this.timeout = timeout;
    }

    @Override
    protected void connect() throws IOException {
        try {
            socket = new Socket();
            socket.connect(server, timeout);
            out = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    protected boolean shouldConnect() {
        return (socket == null || socket.isClosed() || (!socket.isConnected()));
    }

    @Override
    protected void write(byte[] buffer) throws IOException {
        out.write(buffer);
        out.flush();
    }

    @Override
    protected void disconnect() {
        // close output stream
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) { // ignore
            } finally {
                out = null;
            }
        }

        // close socket
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) { // ignore
            } finally {
                socket = null;
            }
        }
    }

    @Override
    protected String getDestinationName() {
        return server.toString();
    }

    @Override
    public String getName() {
        return name;
    }
}
