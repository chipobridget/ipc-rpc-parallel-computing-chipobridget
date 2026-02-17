package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 */
public class Master {
    private ServerSocket serverSocket;
    private int port;
    private String studentId;
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(10);
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final Map<String, TaskResult> results = new ConcurrentHashMap<>();
    private final Map<String, Long> workerHeartbeat = new ConcurrentHashMap<>();
    private final Map<String, String> taskToWorker = new ConcurrentHashMap<>();  // Task ID -> Worker ID
    private final Map<String, Task> activeTasks = new ConcurrentHashMap<>();     // Task ID -> Task
    private static final long HEARTBEAT_TIMEOUT_MS = 5000;

    public Master() {
        this(9999);
    }

    public Master(int port) {
        this.port = port;
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) {
            this.studentId = "student_" + System.currentTimeMillis();
        }
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        return null;
    }

    public void listen(int port) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.running.set(true);
        this.port = serverSocket.getLocalPort();
        System.out.println("Master listening on port " + this.port);
        systemThreads.submit(this::monitorHeartbeats);
        // Submit listener to thread pool so this method returns immediately
        systemThreads.submit(this::acceptConnections);
    }

    private void acceptConnections() {
        while (running.get()) {
            try {
                Socket workerSocket = serverSocket.accept();
                systemThreads.submit(() -> handleWorkerConnection(workerSocket));
            } catch (IOException e) {
                if (running.get()) {
                    System.err.println("Error accepting connection: " + e);
                }
            }
        }
    }

    public void reconcileState() {
        // Ensure system health and consistency
        long now = System.currentTimeMillis();
        List<String> deadWorkers = new ArrayList<>();
        for (Map.Entry<String, Long> entry : workerHeartbeat.entrySet()) {
            if (now - entry.getValue() > HEARTBEAT_TIMEOUT_MS) {
                deadWorkers.add(entry.getKey());
            }
        }
        for (String workerId : deadWorkers) {
            removeWorker(workerId);
        }
    }

    public void shutdown() {
        running.set(false);
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e);
        }
        systemThreads.shutdown();
        taskExecutor.shutdown();
    }

    private void handleWorkerConnection(Socket socket) {
        String workerId = null;
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            Message msg = Message.readFromStream(dis);
            if ("REGISTER_WORKER".equals(msg.type)) {
                workerId = new String(msg.payload, "UTF-8");
                
                // Perform advanced handshake with capability negotiation
                try {
                    performHandshake(dis, dos);
                    System.out.println("Handshake completed with worker: " + workerId);
                } catch (IOException e) {
                    System.err.println("Handshake failed: " + e);
                }
                
                WorkerConnection worker = new WorkerConnection(workerId, socket, dis, dos);
                workers.put(workerId, worker);
                workerHeartbeat.put(workerId, System.currentTimeMillis());
                System.out.println("Worker registered: " + workerId);
                
                // Send ack
                Message ack = new Message("WORKER_ACK", studentId, "CSM218", 1);
                ack.payload = "OK".getBytes();
                ack.writeToStream(dos);
                
                while (running.get()) {
                    try {
                        Message incomingMsg = Message.readFromStream(dis);
                        if (incomingMsg == null) break;
                        if ("TASK_COMPLETE".equals(incomingMsg.type)) {
                            // Extract task ID from payload
                            String taskId = extractTaskId(incomingMsg.payload);
                            results.put(workerId, new TaskResult(workerId, incomingMsg.payload));
                            taskToWorker.remove(taskId);
                            activeTasks.remove(taskId);
                        } else if ("HEARTBEAT".equals(incomingMsg.type)) {
                            workerHeartbeat.put(workerId, System.currentTimeMillis());
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Connection error: " + e);
        } finally {
            if (workerId != null) {
                removeWorker(workerId);
            }
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
    }

    private void removeWorker(String workerId) {
        WorkerConnection worker = workers.remove(workerId);
        workerHeartbeat.remove(workerId);
        if (worker != null) {
            try {
                worker.socket.close();
            } catch (IOException e) {
                System.err.println("Error closing worker socket: " + e);
            }
        }
        
        // RECOVERY MECHANISM: Reassign tasks from failed worker to available workers
        List<String> tasksToReassign = new ArrayList<>();
        for (Map.Entry<String, String> entry : taskToWorker.entrySet()) {
            if (workerId.equals(entry.getValue())) {
                tasksToReassign.add(entry.getKey());
            }
        }
        
        if (!tasksToReassign.isEmpty()) {
            System.out.println("Recovering " + tasksToReassign.size() + " tasks from failed worker: " + workerId);
            for (String taskId : tasksToReassign) {
                Task task = activeTasks.get(taskId);
                if (task != null) {
                    // Find an available worker to reassign this task
                    for (WorkerConnection availableWorker : workers.values()) {
                        try {
                            // Send task reassignment notification
                            Message reassignMsg = new Message("TASK_REASSIGN", studentId, "CSM218", 1);
                            reassignMsg.payload = (taskId + "|" + availableWorker.workerId).getBytes("UTF-8");
                            reassignMsg.writeToStream(availableWorker.dos);
                            taskToWorker.put(taskId, availableWorker.workerId);
                            System.out.println("Reassigned task " + taskId + " to worker " + availableWorker.workerId);
                            break;
                        } catch (IOException e) {
                            System.err.println("Failed to reassign task: " + e);
                        }
                    }
                }
            }
        }
    }

    private void performHandshake(DataInputStream dis, DataOutputStream dos) throws IOException {
        // Send handshake request with capabilities
        Message handshakeMsg = new Message("HANDSHAKE_REQUEST", studentId, "CSM218", 1);
        handshakeMsg.messageType = "HANDSHAKE_REQUEST";
        
        // Build capability payload
        StringBuilder capabilityPayload = new StringBuilder();
        capabilityPayload.append("role:MASTER|");
        capabilityPayload.append("protocol_version:1|");
        capabilityPayload.append("timestamp:").append(System.currentTimeMillis()).append("|");
        capabilityPayload.append("supports_heartbeat:true|");
        capabilityPayload.append("supports_recovery:true");
        
        handshakeMsg.payload = capabilityPayload.toString().getBytes("UTF-8");
        handshakeMsg.writeToStream(dos);
        
        // Read handshake response with timeout
        long startTime = System.currentTimeMillis();
        long timeout = 5000;
        Message responseMsg = null;
        while (System.currentTimeMillis() - startTime < timeout) {
            try {
                responseMsg = Message.readFromStream(dis);
                if ("HANDSHAKE_RESPONSE".equals(responseMsg.messageType) || 
                    "HANDSHAKE_RESPONSE".equals(responseMsg.type)) {
                    break;
                }
            } catch (IOException e) {
                if (System.currentTimeMillis() - startTime >= timeout) {
                    throw e;
                }
            }
        }
        
        if (responseMsg == null || (!("HANDSHAKE_RESPONSE".equals(responseMsg.messageType)) && 
            !("HANDSHAKE_RESPONSE".equals(responseMsg.type)))) {
            throw new IOException("Handshake failed - no response received");
        }
    }

    private void monitorHeartbeats() {
        while (running.get()) {
            try {
                Thread.sleep(1000);
                long now = System.currentTimeMillis();
                List<String> deadWorkers = new ArrayList<>();
                for (Map.Entry<String, Long> entry : workerHeartbeat.entrySet()) {
                    if (now - entry.getValue() > HEARTBEAT_TIMEOUT_MS) {
                        deadWorkers.add(entry.getKey());
                    }
                }
                for (String workerId : deadWorkers) {
                    removeWorker(workerId);
                }
                for (WorkerConnection worker : workers.values()) {
                    try {
                        Message ping = new Message("HEARTBEAT", studentId, "CSM218", 1);
                        ping.payload = "PING".getBytes();
                        ping.writeToStream(worker.dos);
                    } catch (IOException e) {
                        removeWorker(worker.workerId);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static class WorkerConnection {
        String workerId;
        Socket socket;
        DataInputStream dis;
        DataOutputStream dos;
        Task currentTask;
        
        WorkerConnection(String workerId, Socket socket, DataInputStream dis, DataOutputStream dos) {
            this.workerId = workerId;
            this.socket = socket;
            this.dis = dis;
            this.dos = dos;
        }
        
        Task getCurrentTask() {
            return currentTask;
        }
        
        void setCurrentTask(Task task) {
            this.currentTask = task;
        }
    }
    
    // Helper method to extract task ID from payload
    private String extractTaskId(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            String payloadStr = new String(payload, "UTF-8");
            // Assume task ID is the first component separated by ':'
            String[] parts = payloadStr.split(":", 2);
            return parts.length > 0 ? parts[0] : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static class Task {
        String id;
        String operation;
        int[][] data;
        
        Task(String id, String operation, int[][] data) {
            this.id = id;
            this.operation = operation;
            this.data = data;
        }
    }

    private static class TaskResult {
        String workerId;
        byte[] result;
        
        TaskResult(String workerId, byte[] result) {
            this.workerId = workerId;
            this.result = result;
        }
    }
}
