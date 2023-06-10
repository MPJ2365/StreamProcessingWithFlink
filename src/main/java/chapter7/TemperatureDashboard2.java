package chapter7;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

// Same but this app use a tuple2 to represent the output.
public class TemperatureDashboard2 {

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

    ArrayList<CompletableFuture<ValueState<Tuple2<String, Double>>>> futures = new ArrayList<>();
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
        ValueState<Tuple2<String, Double>> res = futures.get(i).get();
        BigDecimal temp = new BigDecimal(Double.toString(res.value().f1));
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

  private static CompletableFuture<ValueState<Tuple2<String, Double>>> queryState(String key, QueryableStateClient client) {
    // jobId of running QueryableStateJob.
    // Execute job "TrackMaximumTemperature" and look up the jobId in logs of running job or the web UI
    String jobId = "49545a23da7bf0d61f1edbe0943f681a";
    return client.getKvState(
      JobID.fromHexString(jobId),
      "maxTemperature",
      key,
      Types.STRING,
      new ValueStateDescriptor<Tuple2<String, Double>>("", Types.TUPLE(TypeInformation.of(String.class), TypeInformation.of(Double.class))));
      //new ValueStateDescriptor<Tuple2<String, Double>>("", TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {})));
  }

}