/**
 *
 *  @author Tracewicz Natalia s33507
 *
 */
package zad1;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class ChatClient {
    private final String host;
    private final int port;
    private final String id;
    private SocketChannel channel;
    private final StringBuilder chatView = new StringBuilder();
    private Thread receiverThread;
    private volatile boolean running = true;

    public ChatClient(String host, int port, String id) {
        this.host = host;
        this.port = port;
        this.id = id;
    }

    public void login() {
        try {
            channel = SocketChannel.open(new InetSocketAddress(host, port));
            channel.configureBlocking(false);
            startReceiver();
            sendRequest("login " + id);
        } catch (IOException e) {
            chatView.append("*** ").append(e).append("\n");
        }
    }

    public void logout() {
        try {
            sendRequest("logout");
            Thread.sleep(50);
            running = false;
            if (channel != null) channel.close();
        } catch (Exception e) {
            chatView.append("*** ").append(e).append("\n");
        }
    }

    public void send(String req) {
        sendRequest(req);
    }

    private void sendRequest(String req) {
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8.encode(req + "\n");
            channel.write(buffer);
        } catch (IOException e) {
            chatView.append("*** ").append(e).append("\n");
        }
    }

    private void startReceiver() {
        receiverThread = new Thread(() -> {
            ByteBuffer buffer = ByteBuffer.allocate(2048);
            try {
                while (running && channel.isOpen()) {
                    buffer.clear();
                    int readBytes = channel.read(buffer);
                    if (readBytes > 0) {
                        buffer.flip();
                        String response = StandardCharsets.UTF_8.decode(buffer).toString();
                        chatView.append(response);
                    } else if (readBytes == -1) {
                        break;
                    }
                }
            } catch (IOException e) {
                if (running) chatView.append("*** ").append(e).append("\n");
            }
        });
        receiverThread.setDaemon(true);
        receiverThread.start();
    }

    public String getChatView() {
        return "=== " + id + " chat view\n" + chatView;
    }
}