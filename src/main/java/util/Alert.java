package util;

public class Alert {

    public String message;
    public long timestamp;

    /**
     * Empty default constructor to comply with Flink's POJO requirements.
     */
    public Alert() { }

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "(" + message + ", " + timestamp + ")";
    }
}