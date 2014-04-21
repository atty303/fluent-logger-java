package org.fluentd.logger;

import org.fluentd.logger.sender.Event;
import org.fluentd.logger.sender.NullSender;
import org.fluentd.logger.util.MockFluentd;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFluentLogger {
    private Logger _logger = LoggerFactory.getLogger(TestFluentLogger.class);

    class FixedThreadManager {
        private final ExecutorService service;

        public FixedThreadManager(int numThreads) {
            service = Executors.newFixedThreadPool(numThreads);
        }

        public void submit(Runnable r) {
            service.submit(r);
        }

        public void join() throws InterruptedException {
            service.shutdown();
            while(!service.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                _logger.debug("waiting ...");
            }
            _logger.trace("Terminating FixedThreadManager");
        }
    }


    @Test
    public void testNormal01() throws Exception {
        // start mock fluentd
        int port = MockFluentd.randomPort();
        String host = "localhost";
        final List<Event> elist = new ArrayList<Event>();
        MockFluentd fluentd = new MockFluentd(port, new MockFluentd.MockProcess() {
            public void process(MessagePack msgpack, Socket socket) throws IOException {
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                try {
                    Unpacker unpacker = msgpack.createUnpacker(in);
                    while (true) {
                        Event e = unpacker.read(Event.class);
                        elist.add(e);
                    }
                    //socket.close();
                } catch (EOFException e) {
                    // ignore
                }
            }
        });

        FixedThreadManager threadManager = new FixedThreadManager(1);
        threadManager.submit(fluentd);

        // start loggers
        FluentLogger logger = FluentLogger.getLogger("testtag", host, port);
        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k1", "v1");
            data.put("k2", "v2");
            logger.log("test01", data);
        }
        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k3", "v3");
            data.put("k4", "v4");
            logger.log("test01", data);
        }

        // close loggers
        logger.close();
        Thread.sleep(2000);

        // close mock fluentd
        fluentd.close();

        // wait for unpacking event data on fluentd
        threadManager.join();

        // check data
        assertEquals(2, elist.size());
        assertEquals("testtag.test01", elist.get(0).tag);
        assertEquals("testtag.test01", elist.get(1).tag);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNormal02() throws Exception {
        int loggerCount = 3;

        // start mock fluentd
        int port = MockFluentd.randomPort();
        String host = "localhost";
        final List[] elists = new List[loggerCount];
        elists[0] = new ArrayList<Event>();
        elists[1] = new ArrayList<Event>();
        elists[2] = new ArrayList<Event>();
        MockFluentd fluentd = new MockFluentd(port, new MockFluentd.MockProcess() {
            public void process(MessagePack msgpack, Socket socket) throws IOException {
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                try {
                    Unpacker unpacker = msgpack.createUnpacker(in);
                    while (true) {
                        Event e = unpacker.read(Event.class);
                        if (e.tag.startsWith("noprefix")) {
                            elists[2].add(e); // no tag prefix
                        } else if (e.tag.startsWith("testtag00")) {
                            elists[0].add(e); // testtag00
                        } else {
                            elists[1].add(e); // testtag01
                        }
                    }
                    //socket.close();
                } catch (EOFException e) {
                    // ignore
                }
            }
        });
        FixedThreadManager threadManager = new FixedThreadManager(1);
        threadManager.submit(fluentd);

        // start loggers
        FluentLogger[] loggers = new FluentLogger[loggerCount];
        int[] counts = new int[] { 50, 100, 75 };
        loggers[0] = FluentLogger.getLogger("testtag00", host, port);
        {
            for (int i = 0; i < counts[0]; i++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("k1", "v1");
                data.put("k2", "v2");
                loggers[0].log("test00", data);
            }
        }
        loggers[1] = FluentLogger.getLogger("testtag01", host, port);
        {
            for (int i = 0; i < counts[1]; i++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("k3", "v3");
                data.put("k4", "v4");
                loggers[1].log("test01", data);
            }
        }
        loggers[2] = FluentLogger.getLogger(null, host, port);
        {
            for (int i = 0; i < counts[2]; i++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("k5", 5555);
                data.put("k6", 6666);
                loggers[2].log("noprefix01", data);
            }
        }

        // close loggers
        FluentLogger.closeAll();
        Thread.sleep(2000);

        // close mock fluentd
        fluentd.close();

        // wait for unpacking event data on fluentd
        threadManager.join();

        // check data
        assertEquals(counts[0], elists[0].size());
        for (Object obj : elists[0]) {
            Event e = (Event) obj;
            assertEquals("testtag00.test00", e.tag);
        }
        assertEquals(counts[1], elists[1].size());
        for (Object obj : elists[1]) {
            Event e = (Event) obj;
            assertEquals("testtag01.test01", e.tag);
        }
        assertEquals(counts[2], elists[2].size());
        for (Object obj : elists[2]) {
            Event e = (Event) obj;
            assertEquals("noprefix01", e.tag);
        }
    }

    @Test
    public void testReconnection() throws Exception {
        // start mock fluentd
        int port = MockFluentd.randomPort();
        String host = "localhost";
        final List<Event> elist1 = new ArrayList<Event>();

        FixedThreadManager threadManager = new FixedThreadManager(2);

        MockFluentd fluentd1 = new MockFluentd(port, new MockFluentd.MockProcess() {
            public void process(MessagePack msgpack, Socket socket) throws IOException {
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                try {
                    Unpacker unpacker = msgpack.createUnpacker(in);
                    while (true) {
                        Event e = unpacker.read(Event.class);
                        elist1.add(e);

                        if (elist1.size() >= 1)
                            break;
                    }
                    socket.close();
                } catch (EOFException e) {
                    // ignore
                }
            }
        });
        threadManager.submit(fluentd1);

        // start loggers
        FluentLogger logger = FluentLogger.getLogger("testtag", host, port);
        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k1", "v1");
            data.put("k2", "v2");
            logger.log("test01", data);
        }

        TimeUnit.MILLISECONDS.sleep(500);
        _logger.info("Closing the current fluentd instance");
        fluentd1.closeClientSockets();
        fluentd1.close();

        TimeUnit.MILLISECONDS.sleep(500);

        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k3", "v3");
            data.put("k4", "v4");
            logger.log("test01", data);
        }

        final List<Event> elist2 = new ArrayList<Event>();
        MockFluentd fluentd2 = new MockFluentd(port, new MockFluentd.MockProcess() {
            public void process(MessagePack msgpack, Socket socket) throws IOException {
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                try {
                    Unpacker unpacker = msgpack.createUnpacker(in);
                    while (true) {
                        Event e = unpacker.read(Event.class);
                        elist2.add(e);
                    }
                    // socket.close();
                } catch (EOFException e) {
                    // ignore
                }
            }
        });
        threadManager.submit(fluentd2);

        TimeUnit.MILLISECONDS.sleep(500);

        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k5", "v5");
            data.put("k6", "v6");
            logger.log("test01", data);
        }

        // close loggers
        FluentLogger.closeAll();
        Thread.sleep(2000);

        fluentd2.close();


        // wait for unpacking event data on fluentd
        TimeUnit.MILLISECONDS.sleep(2000);
        threadManager.join();


        // check data
        assertEquals(1, elist1.size());
        assertEquals("testtag.test01", elist1.get(0).tag);

        assertEquals(2, elist2.size());
        assertEquals("testtag.test01", elist2.get(0).tag);
        assertEquals("testtag.test01", elist2.get(1).tag);
    }

    @Test
    public void testClose() throws Exception {
        // use NullSender
        Properties props = System.getProperties();
        props.setProperty(Config.FLUENT_SENDER_CLASS, NullSender.class.getName());

        // create logger objects
        FluentLogger.getLogger("tag1");
        FluentLogger.getLogger("tag2");
        FluentLogger.getLogger("tag3");

        Map<String, FluentLogger> loggers;
        {
            loggers = FluentLogger.getLoggers();
            assertEquals(3, loggers.size());
        }

        // close and delete
        FluentLogger.closeAll();
        {
            loggers = FluentLogger.getLoggers();
            assertEquals(0, loggers.size());
        }

        props.remove(Config.FLUENT_SENDER_CLASS);
    }

    @Test
    public void testGetLoggerShouldReturnSameInstanceIfExistsStrongReferenceToLoggerDuringRunsGC() throws Exception {
        // use NullSender
        Properties props = System.getProperties();
        props.setProperty(Config.FLUENT_SENDER_CLASS, NullSender.class.getName());

        // get a logger and hold strong reference to it
        FluentLogger logger1 = FluentLogger.getLogger("tag");
        int logger1Identity = System.identityHashCode(FluentLogger.getLogger("tag"));

        System.gc();

        // get a same logger
        int logger2Identity = System.identityHashCode(FluentLogger.getLogger("tag"));

        assertTrue(logger1Identity == logger2Identity);

        props.remove(Config.FLUENT_SENDER_CLASS);
    }

    @Test
    public void testGetLoggerShouldReturnDifferInstanceIfNotExistsStrongReferenceToLoggerDuringRunsGC() throws Exception {
        // use NullSender
        Properties props = System.getProperties();
        props.setProperty(Config.FLUENT_SENDER_CLASS, NullSender.class.getName());

        // get a logger and does not hold reference to it
        int logger1Identity = System.identityHashCode(FluentLogger.getLogger("tag"));

        System.gc();

        // get a same logger
        int logger2Identity = System.identityHashCode(FluentLogger.getLogger("tag"));

        assertTrue(logger1Identity != logger2Identity);

        props.remove(Config.FLUENT_SENDER_CLASS);
    }

}
