package pdc;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 * 
 * Wire Format: Length-prefixed binary encoding
 * [Total Length (4 bytes, big-endian)]
 * [Magic length (2 bytes)] [Magic string]
 * [Version (4 bytes)]
 * [Type length (2 bytes)] [Type string]
 * [Sender length (2 bytes)] [Sender string]
 * [Timestamp (8 bytes)]
 * [Payload length (4 bytes)] [Payload bytes]
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.payload = new byte[0];
    }

    public Message(String type, String sender, String magic, int version) {
        this.type = type;
        this.sender = sender;
        this.magic = magic;
        this.version = version;
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixed binary encoding.
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Reserve space for total length (will write later)
            int lengthPos = baos.size();
            dos.writeInt(0); // Placeholder for total length

            // Write magic
            byte[] magicBytes = (magic != null ? magic : "CSM218").getBytes("UTF-8");
            dos.writeShort(magicBytes.length);
            dos.write(magicBytes);

            // Write version
            dos.writeInt(version);

            // Write type
            byte[] typeBytes = (type != null ? type : "").getBytes("UTF-8");
            dos.writeShort(typeBytes.length);
            dos.write(typeBytes);

            // Write sender
            byte[] senderBytes = (sender != null ? sender : "").getBytes("UTF-8");
            dos.writeShort(senderBytes.length);
            dos.write(senderBytes);

            // Write timestamp
            dos.writeLong(timestamp);

            // Write payload
            byte[] payloadBytes = payload != null ? payload : new byte[0];
            dos.writeInt(payloadBytes.length);
            dos.write(payloadBytes);

            byte[] result = baos.toByteArray();

            // Write total length at the beginning
            ByteBuffer.wrap(result, lengthPos, 4).putInt(result.length - 4);

            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Expects length-prefixed binary format.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 4) {
            return null;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            Message msg = new Message();

            // Read total length (already validated by caller)
            int totalLength = dis.readInt();

            // Read magic
            int magicLen = dis.readShort();
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, "UTF-8");

            // Read version
            msg.version = dis.readInt();

            // Read type
            int typeLen = dis.readShort();
            byte[] typeBytes = new byte[typeLen];
            dis.readFully(typeBytes);
            msg.type = new String(typeBytes, "UTF-8");

            // Read sender
            int senderLen = dis.readShort();
            byte[] senderBytes = new byte[senderLen];
            dis.readFully(senderBytes);
            msg.sender = new String(senderBytes, "UTF-8");

            // Read timestamp
            msg.timestamp = dis.readLong();

            // Read payload
            int payloadLen = dis.readInt();
            if (payloadLen > 0) {
                msg.payload = new byte[payloadLen];
                dis.readFully(msg.payload);
            } else {
                msg.payload = new byte[0];
            }

            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    /**
     * Reads a single message from an input stream (handles framing automatically).
     */
    public static Message readFromStream(DataInputStream dis) throws IOException {
        int totalLength = dis.readInt();
        if (totalLength <= 0) {
            throw new IOException("Invalid message length: " + totalLength);
        }

        byte[] data = new byte[totalLength + 4]; // +4 for the length prefix itself
        ByteBuffer.wrap(data).putInt(totalLength);
        dis.readFully(data, 4, totalLength);

        return unpack(data);
    }

    /**
     * Writes this message to an output stream.
     */
    public void writeToStream(DataOutputStream dos) throws IOException {
        byte[] packed = this.pack();
        dos.write(packed);
        dos.flush();
    }

    @Override
    public String toString() {
        return "Message{" +
                "magic='" + magic + '\'' +
                ", version=" + version +
                ", type='" + type + '\'' +
                ", sender='" + sender + '\'' +
                ", timestamp=" + timestamp +
                ", payloadSize=" + (payload != null ? payload.length : 0) +
                '}';
    }
}
