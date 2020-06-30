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
    Harness harness = new Harness().withKryoMessageSerializer()
        .withGlobalConfiguration("parallelism", "4")
        .withSupplyingIngress(FGraphConstants.REQUEST_INGRESS, new MessageGenerator())
        .withPrintingEgress(FGraphConstants.RESULT_EGRESS);

        harness.start();
  }

  /** generate a random message, once a second a second. */
  private static final class MessageGenerator implements SerializableSupplier<FGraphMessages.InputMessage> {

    private static final long serialVersionUID = 1;

    private BufferedReader br = null;

    @Override
    public FGraphMessages.InputMessage get() {

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

    private FGraphMessages.InputMessage newMessage(String line) {
      String[] strs = line.split("\t");
      if (line.length() < 5) {
        return new FGraphMessages.InputMessage("null", "null", -1L);
      }

      return new FGraphMessages.InputMessage(strs[2], strs[3], Long.parseLong(strs[1]));
    }
  }
}
