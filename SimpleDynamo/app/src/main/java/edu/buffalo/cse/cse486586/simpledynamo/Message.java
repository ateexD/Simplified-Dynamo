package edu.buffalo.cse.cse486586.simpledynamo;

public class Message {
    int myPort;
    String status;
    String key;
    String value;
    Long timestamp;

    public Message(int myPort, String status, String key, String value, long timestamp) {
        this.myPort = myPort;
        this.status = status;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(this.myPort);
        res.append(";");
        res.append(this.status);
        res.append(";");
        res.append(this.key);
        res.append(";");
        res.append(this.value);
        res.append(";");
        res.append(this.timestamp);
        return res.toString();
    }
}
