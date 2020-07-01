package bu.dsp.fgraph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;
import org.junit.Test;

public class RunnerTest {

  @Test
  public void run() throws Exception {
    Harness harness = new Harness().withKryoMessageSerializer().withGlobalConfiguration("parallelism", "4")
        .withSupplyingIngress(FGraphConstants.REQUEST_INGRESS_EDEG, new EdgeGenerator())
        .withSupplyingIngress(FGraphConstants.REQUEST_INGRESS_QUERY, new QueryGenerator())
        .withPrintingEgress(FGraphConstants.RESULT_EGRESS_QUERY_NEIGHBORS);

    harness.start();
  }

  /** generate messages from relations source file. **/
  private static final class EdgeGenerator implements SerializableSupplier<FGraphMessages.EdgeMessage> {
    private static final long serialVersionUID = -7708670958427247564L;
    private BufferedReader br = null;

    @Override
    public FGraphMessages.EdgeMessage get() {

      if (br == null) {
        try {
          br = new BufferedReader(new FileReader("/home/jin/downloads/relations.csv"));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      String line = "";
      try {
        if ((line = br.readLine()) != null) {
          return newMessage(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      return newMessage(line);
    }

    private FGraphMessages.EdgeMessage newMessage(String line) {
      String[] strs = line.split("\t");
      if (line.length() < 5) {
        return new FGraphMessages.EdgeMessage("null", "null", -1L);
      }

      return new FGraphMessages.EdgeMessage(strs[2], strs[3], Long.parseLong(strs[1]));
    }
  }

  /** generate query messages every 3 seconds**/
  private static final class QueryGenerator implements SerializableSupplier<FGraphMessages.QueryMessage> {
    private static final long serialVersionUID = 2675863538043226751L;

    @Override
    public FGraphMessages.QueryMessage get() {
      try {
        Thread.sleep(3_000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return new FGraphMessages.QueryMessage(
        FGraphMessages.QueryMessage.Command.QUERY_NEIGHBORS, 
        "0000001" , 
        new FGraphTypes.QueryNeighborsArgs("0000001", 2, 7857852L, 7949381L));
    }
  }
}
