package util;

public class SensorReading {

    @Override
    public String toString() {
        return "SensorReading{" +
            "id='" + id + '\'' +
            ", timestamp=" + timestamp +
            ", temperature=" + temperature +
            '}';
    }

    String id;
    Long timestamp;
    Double temperature;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public SensorReading() {
    }
}
