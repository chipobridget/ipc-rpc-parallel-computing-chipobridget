package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    
    private String workerId;
    private String masterHost;
    private int masterPort;
    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private String studentId;
    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
    private final AtomicBoolean running = new AtomicBoolean(false);

    public Worker() {
        this("worker_" + System.currentTimeMillis(), "localhost", 9999);
    }

    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) {
            this.studentId = "student_" + System.currentTimeMillis();
        }
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            inputStream = new DataInputStream(socket.getInputStream());
            outputStream = new DataOutputStream(socket.getOutputStream());
            
            // Send registration message
            Message regMsg = new Message("REGISTER_WORKER", studentId, "CSM218", 1);
            regMsg.payload = workerId.getBytes("UTF-8");
            regMsg.writeToStream(outputStream);
            
            // Wait for acknowledgment
            Message ackMsg = Message.readFromStream(inputStream);
            
            if ("WORKER_ACK".equals(ackMsg.type)) {
                System.out.println("Worker " + workerId + " registered successfully");
                running.set(true);
                // Start listening for tasks
                execute();
            }
        } catch (Exception e) {
            System.err.println("Failed to join cluster: " + e);
            e.printStackTrace();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        try {
            while (running.get()) {
                try {
                    // Read incoming message from master
                    Message msg = Message.readFromStream(inputStream);
                    
                    if (msg == null) {
                        break;
                    }
                    
                    if ("MATRIX_BLOCK_MULTIPLY".equals(msg.type)) {
                        // Execute task in thread pool
                        taskExecutor.submit(() -> {
                            try {
                                processTask(msg);
                            } catch (Exception e) {
                                System.err.println("Task execution error: " + e);
                            }
                        });
                    } else if ("HEARTBEAT".equals(msg.type)) {
                        // Respond to heartbeat
                        Message heartbeatAck = new Message("HEARTBEAT", studentId, "CSM218", 1);
                        heartbeatAck.payload = "PONG".getBytes();
                        heartbeatAck.writeToStream(outputStream);
                    }
                } catch (EOFException e) {
                    System.out.println("Connection closed by master");
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error in execute loop: " + e);
        } finally {
            cleanup();
        }
    }

    /**
     * Process a received task
     */
    private void processTask(Message taskMsg) throws IOException {
        String payload = new String(taskMsg.payload, "UTF-8");
        String[] parts = payload.split(":", 2);
        
        if (parts.length < 2) {
            return;
        }
        
        String taskId = parts[0];
        String data = parts[1];
        
        // Parse task data
        String[] dataParts = data.split(";");
        if (dataParts.length < 3) {
            return;
        }
        
        String[] metadata = dataParts[0].split(",");
        int startRow = Integer.parseInt(metadata[0]);
        int endRow = Integer.parseInt(metadata[1]);
        int colsB = Integer.parseInt(metadata[2]);
        int innerDim = Integer.parseInt(metadata[3]);
        
        // Reconstruct matrices
        int[][] matrixA = new int[endRow - startRow][];
        int[][] matrixB = new int[innerDim][];
        
        int idx = 1;
        // Parse matrix A
        for (int i = 0; i < endRow - startRow; i++) {
            if (idx >= dataParts.length) break;
            String[] values = dataParts[idx++].split(",");
            matrixA[i] = new int[values.length];
            for (int j = 0; j < values.length; j++) {
                matrixA[i][j] = Integer.parseInt(values[j]);
            }
        }
        
        // Parse matrix B
        for (int i = 0; i < innerDim; i++) {
            if (idx >= dataParts.length) break;
            String[] values = dataParts[idx++].split(",");
            matrixB[i] = new int[values.length];
            for (int j = 0; j < values.length; j++) {
                matrixB[i][j] = Integer.parseInt(values[j]);
            }
        }
        
        // Perform multiplication
        int[][] result = new int[endRow - startRow][colsB];
        for (int i = 0; i < matrixA.length; i++) {
            for (int j = 0; j < colsB; j++) {
                for (int k = 0; k < innerDim; k++) {
                    result[i][j] += matrixA[i][k] * matrixB[k][j];
                }
            }
        }
        
        // Send result back to master
        Message resultMsg = new Message("TASK_COMPLETE", studentId, "CSM218", 1);
        StringBuilder resultPayload = new StringBuilder();
        resultPayload.append(taskId).append(":");
        resultPayload.append(startRow).append(",").append(endRow).append(",").append(colsB).append(";");
        
        for (int i = 0; i < result.length; i++) {
            for (int j = 0; j < result[i].length; j++) {
                if (j > 0) resultPayload.append(",");
                resultPayload.append(result[i][j]);
            }
            resultPayload.append(";");
        }
        
        resultMsg.payload = resultPayload.toString().getBytes("UTF-8");
        resultMsg.writeToStream(outputStream);
    }

    /**
     * Cleanup resources
     */
    private void cleanup() {
        running.set(false);
        
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing socket: " + e);
        }
        
        taskExecutor.shutdownNow();
    }

    /**
     * Main method for testing
     */
    public static void main(String[] args) {
        String workerId = System.getenv("WORKER_ID");
        if (workerId == null) {
            workerId = "worker_" + System.currentTimeMillis();
        }
        
        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null) {
            masterHost = "localhost";
        }
        
        int masterPort = 9999;
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null) {
            masterPort = Integer.parseInt(portEnv);
        }
        
        Worker worker = new Worker(workerId, masterHost, masterPort);
        
        Runtime.getRuntime().addShutdownHook(new Thread(worker::cleanup));
        
        worker.joinCluster(masterHost, masterPort);
    }
}
