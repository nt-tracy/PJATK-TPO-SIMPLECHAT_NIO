/**
 *
 *  @author Tracewicz Natalia s33507
 *
 */

package zad1;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ChatServer {

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final StringBuilder serverLog = new StringBuilder();
    private final Map<SocketChannel, String> clients = new HashMap<>();
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private volatile boolean running = false;
    private Thread serverThread;

    public ChatServer(String host, int port) {
        try {
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().setReuseAddress(true);
            serverChannel.bind(new InetSocketAddress(host, port));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            throw new RuntimeException("Nie można zainicjować serwera", e);
        }
    }

    public void startServer() {
        running = true;
        serverThread = new Thread(() -> {
            System.out.println("Server started");
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    if (selector.select() == 0) continue;

                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> iter = selectedKeys.iterator();

                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        iter.remove();

                        if (key.isValid() && key.isAcceptable()) {
                            handleAccept();
                        }
                        if (key.isValid() && key.isReadable()) {
                            handleRead(key);
                        }
                    }
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Main Thread error");
                    }
                }
            }
        });
        serverThread.start();
    }

    public void stopServer() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
        if (serverThread != null) {
            try {
                serverThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            if (serverChannel != null) serverChannel.close();
            if (selector != null) selector.close();
        } catch (IOException e) {
        }
        System.out.println("Server stopped");
    }

    private void handleAccept() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ);
    }

    private void handleRead(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        try {
            int bytesRead = clientChannel.read(buffer);

            if (bytesRead == -1) {
                disconnectClient(key, clientChannel);
                return;
            }

            buffer.flip();
            String request = StandardCharsets.UTF_8.decode(buffer).toString().trim();

            for (String r : request.split("\n")) {
                if (!r.isEmpty()) processCommand(clientChannel, r);
            }

        } catch (IOException e) {
            disconnectClient(key, clientChannel);
        }
    }

    private void processCommand(SocketChannel sc, String request) {
        String response = "";
        if (request.startsWith("login ")) {
            String id = request.substring(6).trim();
            clients.put(sc, id);
            response = id + " logged in";
        } else if (request.equals("logout")) {
            String id = clients.get(sc);
            if (id != null) {
                response = id + " logged out";
                broadcast(response);
                addToLog(response);
                clients.remove(sc);
            }
            return;
        } else {
            String id = clients.get(sc);
            if (id != null) {
                response = id + ": " + request;
            }
        }

        if (!response.isEmpty()) {
            broadcast(response);
            addToLog(response);
        }
    }


    private void broadcast(String message) {
        ByteBuffer buffer = StandardCharsets.UTF_8.encode(message + "\n");
        Iterator<Map.Entry<SocketChannel, String>> it = clients.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<SocketChannel, String> entry = it.next();
            SocketChannel ch = entry.getKey();
            try {
                if (ch.isOpen()) {
                    ch.write(buffer.duplicate());
                }
            } catch (IOException e) {
                try { ch.close(); } catch (IOException ignored) {}
                it.remove();
            }
        }
    }

    private void disconnectClient(SelectionKey key, SocketChannel channel) {
        String id = clients.get(channel);
        if (id != null) {
            String msg = id + " logged out";
            broadcast(msg);
            addToLog(msg);
            clients.remove(channel);
        }
        try {
            key.cancel();
            channel.close();
        } catch (IOException ignored) {}
    }

    private synchronized void addToLog(String message) {
        String timestamp = LocalTime.now().format(timeFormatter);
        serverLog.append(timestamp).append(" ").append(message).append("\n");
    }

    public String getServerLog() {
        return serverLog.toString();
    }
}