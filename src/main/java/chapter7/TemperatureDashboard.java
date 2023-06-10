package chapter7;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import util.MaxTemperature;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class TemperatureDashboard {

  public static void main(String[] args) throws Exception {

    // configure client with host and port of queryable state proxy
    // Default port
    int proxyPort = 9069;
    // queryable state proxy connection information.
    // can be looked up in logs of running QueryableStateJob
    String proxyHost = "127.0.0.1";
    QueryableStateClient client = new QueryableStateClient(proxyHost, proxyPort);

    // how many sensors to query
    int numSensors = 5;

    ArrayList<CompletableFuture<ValueState<MaxTemperature>>> futures = new ArrayList<>();
    ArrayList<Double> results = new ArrayList<>();

    // print header line of dashboard table
    StringBuilder header = new StringBuilder();

    for (int i = 0; i < numSensors; i++) {
      header.append("sensor_").append(i + 1).append("\t| ");
    }
    System.out.println(header);

    // loop forever
    while (true) {

      // send out async queries
      for (int i = 0; i < numSensors; i++) {
        if (futures.size() <= i) futures.add(queryState("sensor_" + (i + 1), client));
        else futures.set(i, queryState("sensor_" + (i + 1), client));
      }
      // wait for results
      for (int i = 0; i < numSensors; i++) {
        ValueState<MaxTemperature> res = futures.get(i).get();
        BigDecimal temp = new BigDecimal(Double.toString(res.value().temperature));
        temp = temp.setScale(3, RoundingMode.HALF_DOWN);

        if (results.size() <= i) results.add(temp.doubleValue());
        else results.set(i, temp.doubleValue());
      }
      // print result
      StringBuilder line = new StringBuilder();
      for (Double t: results) line.append("\t").append(t).append("\t| ");
      System.out.println(line);

      // wait to send out next queries
      // how often to query
      int refreshInterval = 10000;
      Thread.sleep(refreshInterval);
    }

    // client.shutdownAndWait();

  }

  private static CompletableFuture<ValueState<MaxTemperature>> queryState(String key, QueryableStateClient client) {

    // jobId of running QueryableStateJob.
    // Execute job "TrackMaximumTemperature" and look up the jobId in logs of running job or the web UI
    String jobId = "d055d8b69f0d262606911ec06224a203";
    return client.getKvState(
            JobID.fromHexString(jobId),
      "maxTemperature",
            key,
            Types.STRING,
            new ValueStateDescriptor<>("", TypeInformation.of(MaxTemperature.class).createSerializer(new ExecutionConfig()))
            //new ValueStateDescriptor<>("", TypeInformation.of(new TypeHint<MaxTemperature>() {}).createSerializer(new ExecutionConfig()))
    );
      //new ValueStateDescriptor<MaxTemperature>("", TypeInformation.of(new TypeHint<MaxTemperature>() {})));
  }

}