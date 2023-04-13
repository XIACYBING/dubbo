package org.apache.dubbo.spring.boot.mock;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * 嵌入式Zookeeper
 *
 * @author wang.yubin
 * @since 2023/4/13
 */
public class EmbeddedZooKeeper {

    private static final Random RANDOM = new Random();

    /**
     * Logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedZooKeeper.class);

    /**
     * ZooKeeper client port. This will be determined dynamically upon startup.
     */
    private final int clientPort;

    /**
     * Thread for running the ZooKeeper server.
     */
    private volatile Thread zkServerThread;

    /**
     * ZooKeeper server.
     */
    private volatile ZooKeeperServerMain zkServer;

    private boolean daemon = true;

    /**
     * Construct an EmbeddedZooKeeper with a random port.
     */
    public EmbeddedZooKeeper() {
        clientPort = findRandomPort(30000, 65535);
    }

    /**
     * Construct an EmbeddedZooKeeper with the provided port.
     *
     * @param clientPort port for ZooKeeper server to bind to
     */
    public EmbeddedZooKeeper(int clientPort, boolean daemon) {
        this.clientPort = clientPort;
        this.daemon = daemon;
    }

    /**
     * Returns the port that clients should use to connect to this embedded server.
     *
     * @return dynamically determined client port
     */
    public int getClientPort() {
        return this.clientPort;
    }

    /**
     * Start the ZooKeeper server in a background thread.
     */
    public synchronized void start() {
        if (zkServerThread == null) {
            zkServerThread = new Thread(new ServerRunnable(), "ZooKeeper Server Starter");
            zkServerThread.setDaemon(daemon);
            zkServerThread.start();
        }
    }

    /**
     * Shutdown the ZooKeeper server.
     */
    public synchronized void stop() {
        if (zkServerThread != null) {
            // The shutdown method is protected...thus this hack to invoke it.
            // This will log an exception on shutdown; see
            // https://issues.apache.org/jira/browse/ZOOKEEPER-1873 for details.
            try {
                Method shutdown = ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
                shutdown.setAccessible(true);
                shutdown.invoke(zkServer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // It is expected that the thread will exit after
            // the server is shutdown; this will block until
            // the shutdown is complete.
            try {
                zkServerThread.join(5000);
                zkServerThread = null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for embedded ZooKeeper to exit");
                // abandoning zk thread
                zkServerThread = null;
            }
        }
    }

    /**
     * Runnable implementation that starts the ZooKeeper server.
     */
    private class ServerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                Properties properties = new Properties();
                File file = new File(System.getProperty("java.io.tmpdir")
                    + File.separator + UUID.randomUUID());
                file.deleteOnExit();
                properties.setProperty("dataDir", file.getAbsolutePath());
                properties.setProperty("clientPort", String.valueOf(clientPort));

                QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
                quorumPeerConfig.parseProperties(properties);

                zkServer = new ZooKeeperServerMain();
                ServerConfig configuration = new ServerConfig();
                configuration.readFrom(quorumPeerConfig);

                zkServer.runFromConfig(configuration);
            } catch (Exception e) {
                logger.error("Exception running embedded ZooKeeper", e);
            }
        }
    }

    /**
     * Workaround for SocketUtils.findRandomPort() deprecation.
     *
     * @param min min port
     * @param max max port
     * @return a random generated available port
     */
    private static int findRandomPort(int min, int max) {
        if (min < 1024) {
            throw new IllegalArgumentException("Max port shouldn't be less than 1024.");
        }

        if (max > 65535) {
            throw new IllegalArgumentException("Max port shouldn't be greater than 65535.");
        }

        if (min > max) {
            throw new IllegalArgumentException("Min port shouldn't be greater than max port.");
        }

        int port = 0;
        int counter = 0;

        // Workaround for legacy JDK doesn't support Random.nextInt(min, max).
        List<Integer> randomInts = RANDOM.ints(min, max + 1)
                                         .limit(max - min)
                                         .mapToObj(Integer::valueOf)
                                         .collect(Collectors.toList());

        do {
            if (counter > max - min) {
                throw new IllegalStateException("Unable to find a port between " + min + "-" + max);
            }

            port = randomInts.get(counter);
            counter++;
        } while (isPortInUse(port));

        return port;
    }

    private static boolean isPortInUse(int port) {
        try (ServerSocket ignored = new ServerSocket(port)) {
            return false;
        } catch (IOException e) {
            // continue
        }
        return true;
    }

}
