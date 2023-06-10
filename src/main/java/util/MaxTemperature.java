package util;

import java.io.Serializable;

public class MaxTemperature implements Serializable {

    public String id;
    public Double temperature;

    public MaxTemperature() {
    }

    public MaxTemperature(String id, Double temperature) {
        this.id = id;
        this.temperature = temperature;
    }

}
