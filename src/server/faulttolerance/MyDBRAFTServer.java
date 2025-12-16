package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import server.ReplicatedServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Raft-based fault-tolerant server using Apache Ratis.
 * This implementation provides:
 * - Strong consistency through Raft consensus
 * - Fault tolerance with automatic leader election
 * - State machine replication
 * - Checkpointing for efficient recovery
 */
public class MyDBRAFTServer extends server.MyDBSingleServer {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    private static final int MAX_LOG_SIZE = 400;
    private static final long CLIENT_WAIT_MS = 30_000L;
    private static final int CHECKPOINT_INTERVAL = 100; // Checkpoint every 100 requests

    private static Cluster cluster = null;
    private final Session session;
    private final String keyspace;
    private final RaftServer raftServer;
    private final RaftGroup raftGroup;
    private RaftPeerId localPeerId;
    private final DBStateMachine stateMachine;

    private final ConcurrentHashMap<String, CompletableFuture<String>> pendingRequests = new ConcurrentHashMap<>();
    private final ScheduledExecutorService checkpointExecutor = Executors.newSingleThreadScheduledExecutor();

    public MyDBRAFTServer(InetSocketAddress isa, InetSocketAddress isaDB, String keyspace, Cluster cluster, Session session, String keyspace1, DBStateMachine stateMachine) throws IOException {
        super(isa, isaDB, keyspace);
        this.cluster = cluster;
        this.session = session;
        this.keyspace = keyspace1;
        this.stateMachine = stateMachine;
    }


    /**
     * State machine that applies commands to Cassandra after Raft consensus.
     */
    private class DBStateMachine extends BaseStateMachine {
        private final AtomicLong lastAppliedIndex = new AtomicLong(0);
        private final Object applyLock = new Object();

        @Override
        public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
            super.initialize(server, groupId, storage);
            setStateMachineStorage(new SimpleStateMachineStorage());

            // Recover last applied index from Cassandra
            try {
                ResultSet rs = session.execute(
                        "SELECT last_applied FROM " + keyspace + ".raft_meta WHERE id='meta'");
                Row r = rs.one();
                if (r != null) {
                    long recovered = r.getLong("last_applied");
                    lastAppliedIndex.set(recovered);
                    System.out.println("[" + keyspace + "] Recovered lastAppliedIndex: " + recovered);
                }
            } catch (Exception e) {
                System.err.println("[" + keyspace + "] Failed to recover lastAppliedIndex: " + e.getMessage());
            }
        }

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            RaftProtos.LogEntryProto entry = trx.getLogEntry();
            long index = entry.getIndex();

            synchronized (applyLock) {
                // Ensure sequential application
                if (index <= lastAppliedIndex.get()) {
                    System.out.println("[" + keyspace + "] Skipping already applied index: " + index);
                    return CompletableFuture.completedFuture(Message.EMPTY);
                }

                ByteString data = entry.getStateMachineLogEntry().getLogData();
                String command = data.toStringUtf8();

                System.out.println("[" + keyspace + "] Applying index " + index + ": " + command);

                try {
                    // Parse command: format is "reqId::cql"
                    String[] parts = command.split("::", 2);
                    String reqId = parts[0];
                    String cql = parts.length > 1 ? parts[1] : "";

                    if (!cql.isEmpty()) {
                        // Qualify CQL with keyspace if needed
                        String qualifiedCql = qualifyCql(cql);
                        session.execute(qualifiedCql);
                        System.out.println("[" + keyspace + "] Successfully executed: " + qualifiedCql);
                    }

                    lastAppliedIndex.set(index);
                    persistLastApplied(index);

                    // Complete pending request if any
                    CompletableFuture<String> future = pendingRequests.remove(reqId);
                    if (future != null) {
                        future.complete("SUCCESS");
                    }

                    // Checkpoint periodically
                    if (index % CHECKPOINT_INTERVAL == 0) {
                        takeSnapshot(index);
                    }

                    return CompletableFuture.completedFuture(Message.EMPTY);

                } catch (Exception e) {
                    System.err.println("[" + keyspace + "] Error applying transaction at index " + index + ": " + e.getMessage());
                    e.printStackTrace();

                    // Complete with error
                    String[] parts = command.split("::", 2);
                    String reqId = parts[0];
                    CompletableFuture<String> future = pendingRequests.remove(reqId);
                    if (future != null) {
                        future.completeExceptionally(e);
                    }

                    return CompletableFuture.failedFuture(e);
                }
            }
        }

        @Override
        public long takeSnapshot() {
            long index = lastAppliedIndex.get();
            takeSnapshot(index);
            return index;
        }

        private void takeSnapshot(long index) {
            try {
                System.out.println("[" + keyspace + "] Taking snapshot at index: " + index);

                // Read all data from grade table
                ResultSet rs = session.execute("SELECT * FROM " + keyspace + ".grade");
                List<Map<String, Object>> snapshot = new ArrayList<>();

                for (Row row : rs) {
                    Map<String, Object> record = new HashMap<>();
                    record.put("id", row.getInt("id"));
                    record.put("events", row.getList("events", Integer.class));
                    snapshot.add(record);
                }

                // Serialize snapshot to file
                Path snapshotDir = getStateMachineDir().resolve("snapshots");
                Files.createDirectories(snapshotDir);
                Path snapshotFile = snapshotDir.resolve("snapshot-" + index + ".dat");

                try (ObjectOutputStream oos = new ObjectOutputStream(
                        new FileOutputStream(snapshotFile.toFile()))) {
                    oos.writeLong(index);
                    oos.writeObject(snapshot);
                }

                System.out.println("[" + keyspace + "] Snapshot saved: " + snapshotFile);

                // Clean up old snapshots
                cleanupOldSnapshots(snapshotDir, index);

            } catch (Exception e) {
                System.err.println("[" + keyspace + "] Failed to take snapshot: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void cleanupOldSnapshots(Path snapshotDir, long currentIndex) {
            try {
                Files.list(snapshotDir)
                        .filter(p -> p.getFileName().toString().startsWith("snapshot-"))
                        .filter(p -> {
                            try {
                                String name = p.getFileName().toString();
                                long idx = Long.parseLong(name.substring(9, name.length() - 4));
                                return idx < currentIndex - CHECKPOINT_INTERVAL;
                            } catch (Exception e) {
                                return false;
                            }
                        })
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                                System.out.println("[" + keyspace + "] Deleted old snapshot: " + p);
                            } catch (IOException e) {
                                System.err.println("[" + keyspace + "] Failed to delete snapshot: " + e.getMessage());
                            }
                        });
            } catch (Exception e) {
                System.err.println("[" + keyspace + "] Failed to cleanup snapshots: " + e.getMessage());
            }
        }

        @Override
        public void pause() {
            // Pause applying new transactions during snapshot
        }

        @Override
        public void reinitialize() throws IOException {
            // Load latest snapshot
            try {
                Path snapshotDir = getStateMachineDir().resolve("snapshots");
                if (!Files.exists(snapshotDir)) {
                    return;
                }

                Optional<Path> latestSnapshot = Files.list(snapshotDir)
                        .filter(p -> p.getFileName().toString().startsWith("snapshot-"))
                        .max(Comparator.comparing(p -> {
                            try {
                                String name = p.getFileName().toString();
                                return Long.parseLong(name.substring(9, name.length() - 4));
                            } catch (Exception e) {
                                return 0L;
                            }
                        }));

                if (latestSnapshot.isPresent()) {
                    loadSnapshot(latestSnapshot.get());
                }

            } catch (Exception e) {
                System.err.println("[" + keyspace + "] Failed to reinitialize: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @SuppressWarnings("unchecked")
        private void loadSnapshot(Path snapshotFile) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(snapshotFile.toFile()))) {

                long index = ois.readLong();
                List<Map<String, Object>> snapshot = (List<Map<String, Object>>) ois.readObject();

                System.out.println("[" + keyspace + "] Loading snapshot from index: " + index);

                // Clear existing data
                session.execute("TRUNCATE " + keyspace + ".grade");

                // Restore data
                for (Map<String, Object> record : snapshot) {
                    int id = (int) record.get("id");
                    List<Integer> events = (List<Integer>) record.get("events");

                    StringBuilder sb = new StringBuilder();
                    sb.append("INSERT INTO ").append(keyspace).append(".grade (id, events) VALUES (");
                    sb.append(id).append(", [");
                    for (int i = 0; i < events.size(); i++) {
                        if (i > 0) sb.append(", ");
                        sb.append(events.get(i));
                    }
                    sb.append("])");

                    session.execute(sb.toString());
                }

                lastAppliedIndex.set(index);
                persistLastApplied(index);

                System.out.println("[" + keyspace + "] Snapshot loaded successfully. Records restored: " + snapshot.size());

            } catch (Exception e) {
                System.err.println("[" + keyspace + "] Failed to load snapshot: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private Path getStateMachineDir() {
            return Paths.get("raft_data", keyspace, "statemachine");
        }

        private void persistLastApplied(long index) {
            try {
                session.execute("UPDATE " + keyspace + ".raft_meta SET last_applied = " + index + " WHERE id='meta'");
            } catch (Exception e) {
                System.err.println("[" + keyspace + "] Failed to persist lastApplied: " + e.getMessage());
            }
        }
    }

    public MyDBRAFTServer(NodeConfig<String> nodeConfig,
                                       String myID,
                                       InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(
                        nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB,
                myID);

        this.keyspace = myID;

        // Initialize Cassandra connection
        try {
            String cassHost = isaDB.getAddress().getHostAddress();
            int cassPort = isaDB.getPort();
            this.cluster = Cluster.builder()
                    .addContactPoint(cassHost)
                    .withPort(cassPort)
                    .build();
            this.session = cluster.connect(this.keyspace);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Cassandra keyspace " + this.keyspace, e);
        }

        // Create tables
        try {
            String createGradeTable = "CREATE TABLE IF NOT EXISTS " + keyspace + ".grade " +
                    "(id int PRIMARY KEY, events list<int>)";
            session.execute(createGradeTable);
            System.out.println("[" + keyspace + "] Created grade table");

            String createMetaTable = "CREATE TABLE IF NOT EXISTS " + keyspace + ".raft_meta " +
                    "(id text PRIMARY KEY, last_applied bigint)";
            session.execute(createMetaTable);

            ResultSet rs = session.execute(
                    "SELECT last_applied FROM " + keyspace + ".raft_meta WHERE id='meta'");
            Row r = rs.one();
            if (r == null) {
                session.execute("INSERT INTO " + keyspace + ".raft_meta (id, last_applied) VALUES ('meta', 0)");
                System.out.println("[" + keyspace + "] Initialized raft_meta");
            }
        } catch (Exception e) {
            System.err.println("[" + keyspace + "] ERROR creating tables: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to create tables: " + e.getMessage(), e);
        }

        // Build Raft group with all servers
        List<RaftPeer> peers = new ArrayList<>();
        Set<String> allServers = nodeConfig.getNodeIDs();

        for (String serverId : allServers) {
            String host = String.valueOf(nodeConfig.getNodeAddress(serverId));
            int basePort = nodeConfig.getNodePort(serverId);
            int raftPort = basePort + 1000; // Offset for Raft communication

            RaftPeerId peerId = RaftPeerId.valueOf(serverId);
            RaftPeer peer = RaftPeer.newBuilder()
                    .setId(peerId)
                    .setAddress(host + ":" + raftPort)
                    .build();
            peers.add(peer);

            if (serverId.equals(myID)) {
                this.localPeerId = peerId;
            }
        }

        this.localPeerId = RaftPeerId.valueOf(myID);
        RaftGroupId groupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes("DBRaftGroup".getBytes()));
        this.raftGroup = RaftGroup.valueOf(groupId, peers);

        // Configure Raft properties
        RaftProperties properties = new RaftProperties();

        // Server configuration
        int raftPort = nodeConfig.getNodePort(myID) + 1000;
        GrpcConfigKeys.Server.setPort(properties, raftPort);

        // Storage directory
        Path storagePath = Paths.get("raft_data", keyspace);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storagePath.toFile()));

        // Performance tuning
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, java.time.Duration.ofMillis(300));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, java.time.Duration.ofMillis(500));
        RaftServerConfigKeys.Log.setSegmentSizeMax(properties, 1024 * 1024); // 1MB segments
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, CHECKPOINT_INTERVAL);

        // Initialize state machine
        this.stateMachine = new DBStateMachine();

        // Build and start Raft server
        try {
            this.raftServer = RaftServer.newBuilder()
                    .setGroup(raftGroup)
                    .setProperties(properties)
                    .setServerId(localPeerId)
                    .setStateMachine(stateMachine)
                    .build();

            this.raftServer.start();
            System.out.println("[" + keyspace + "] Raft server started on port " + raftPort);

        } catch (Exception e) {
            session.close();
            cluster.close();
            throw new IOException("Failed to start Raft server: " + e.getMessage(), e);
        }

        // Start periodic checkpoint
        checkpointExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (raftServer.getLifeCycleState() == LifeCycle.State.RUNNING) {
                    stateMachine.takeSnapshot();
                }
            } catch (Throwable t) {
                System.err.println("[" + keyspace + "] Error in checkpoint task: " + t.getMessage());
            }
        }, CHECKPOINT_INTERVAL, CHECKPOINT_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String message = new String(bytes, StandardCharsets.UTF_8).trim();
        System.out.println("[" + keyspace + "] Received from client: " + message);

        if (message.isEmpty()) {
            try {
                clientMessenger.send(header.sndr, "".getBytes(StandardCharsets.UTF_8));
            } catch (IOException ignore) {}
            return;
        }

        String reqID;
        String cql;
        if (!message.contains("::")) {
            reqID = UUID.randomUUID().toString();
            cql = message.trim();
        } else {
            String[] parts = message.split("::", 2);
            reqID = parts[0].trim();
            cql = parts[1].trim();
        }

        if (cql.isEmpty()) {
            try {
                String resp = reqID + "::Empty query";
                clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
            } catch (IOException ignore) {}
            return;
        }

        System.out.println("[" + keyspace + "] Processing CQL: " + cql);

        try {
            // Submit to Raft
            String command = reqID + "::" + cql;
            ByteString data = ByteString.copyFrom(command.getBytes(StandardCharsets.UTF_8));
            Message message_obj = Message.valueOf(data);

            CompletableFuture<String> future = new CompletableFuture<>();
            pendingRequests.put(reqID, future);

            // Submit to Raft leader
            RaftClientReply reply = raftServer.submitClientRequestAsync(
                    RaftClientRequest.newBuilder()
                            .setClientId(ClientId.randomId())
                            .setServerId(localPeerId)
                            .setGroupId(raftGroup.getGroupId())
                            .setCallId(System.currentTimeMillis())
                            .setMessage(message_obj)
                            .setType(RaftClientRequest.writeRequestType())
                            .build()
            ).get(CLIENT_WAIT_MS, TimeUnit.MILLISECONDS);

            if (reply.isSuccess()) {
                // Wait for state machine to apply
                try {
                    future.get(CLIENT_WAIT_MS, TimeUnit.MILLISECONDS);
                    String resp = reqID + "::Execution Completed!";
                    clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
                } catch (TimeoutException e) {
                    String resp = reqID + "::ERR";
                    clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
                }
            } else {
                String resp = reqID + "::ERR";
                clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
            }

        } catch (Exception e) {
            System.err.println("[" + keyspace + "] Error submitting to Raft: " + e.getMessage());
            e.printStackTrace();
            try {
                String resp = reqID + "::ERR";
                clientMessenger.send(header.sndr, resp.getBytes(StandardCharsets.UTF_8));
            } catch (IOException ignore) {}
        } finally {
            pendingRequests.remove(reqID);
        }
    }

    @Override
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // Not needed - Raft handles server-to-server communication
    }

    private String qualifyCql(String cql) {
        String lowerCql = cql.toLowerCase();
        if (lowerCql.contains(keyspace.toLowerCase() + ".")) {
            return cql;
        }

        String qualified = cql.replaceAll("(?i)\\b(insert\\s+into)\\s+(grade|raft_meta)(\\s|$)",
                "$1 " + keyspace + ".$2$3");
        qualified = qualified.replaceAll("(?i)\\b(update)\\s+(grade|raft_meta)(\\s|$)",
                "$1 " + keyspace + ".$2$3");
        qualified = qualified.replaceAll("(?i)\\b(from)\\s+(grade|raft_meta)(\\s|$)",
                "$1 " + keyspace + ".$2$3");
        qualified = qualified.replaceAll("(?i)\\b(truncate)\\s+(grade|raft_meta)(\\s|$)",
                "$1 " + keyspace + ".$2$3");
        qualified = qualified.replaceAll("(?i)\\b(drop\\s+table)\\s+(grade|raft_meta)(\\s|$)",
                "$1 " + keyspace + ".$2$3");
        qualified = qualified.replaceAll("(?i)\\b(create\\s+table)\\s+(grade|raft_meta)(\\s|$)",
                "$1 " + keyspace + ".$2$3");
        qualified = qualified.replaceAll("(?i)\\btable\\s+if\\s+not\\s+exists\\s+(grade|raft_meta)(\\s|$)",
                "table if not exists " + keyspace + ".$2$3");

        return qualified;
    }

    @Override
    public void close() {
        try {
            checkpointExecutor.shutdownNow();
        } catch (Exception ignored) {}

        try {
            if (raftServer != null) {
                raftServer.close();
            }
        } catch (Exception e) {
            System.err.println("[" + keyspace + "] Error closing Raft server: " + e.getMessage());
        }

        try {
            session.close();
            cluster.close();
        } catch (Exception ignored) {}

        try {
            super.close();
        } catch (Exception ignore) {}
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: MyDBFaultTolerantServerRaft <config_file> <server_name>");
            System.exit(1);
        }

        String configFile = args[0];
        String serverName = args[1];

        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
        } catch (IOException e) {
            System.err.println("Failed to load config file: " + configFile);
            e.printStackTrace();
            System.exit(1);
        }

        String propertyKey = "server." + serverName;
        String serverAddress = props.getProperty(propertyKey);

        if (serverAddress == null) {
            System.err.println("Server property '" + propertyKey + "' not found in config file " + configFile);
            System.exit(1);
        }

        String[] parts = serverAddress.split(":");
        if (parts.length != 2) {
            System.err.println("Invalid server address format: " + serverAddress);
            System.exit(1);
        }

        NodeConfig<String> nodeConfig;
        try {
            nodeConfig = edu.umass.cs.nio.nioutils.NodeConfigUtils.getNodeConfigFromFile(
                    configFile,
                    ReplicatedServer.SERVER_PREFIX,
                    ReplicatedServer.SERVER_PORT_OFFSET);
        } catch (IOException e) {
            System.err.println("Failed to load node config: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return;
        }

        try {
            System.out.println("Starting MyDBFaultTolerantServerRaft: " + serverName);
            MyDBRAFTServer server = new MyDBRAFTServer(
                    nodeConfig,
                    serverName,
                    new InetSocketAddress("localhost", 9042));
            System.out.println("MyDBFaultTolerantServerRaft " + serverName + " started successfully");
        } catch (Exception e) {
            System.err.println("Failed to start MyDBFaultTolerantServerRaft: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}