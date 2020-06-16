package bu.dsp.fgraph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;
import org.junit.Ignore;
import org.junit.Test;

public class RunnerTest {

  // @Ignore(
  // "This has an infinite egress and it would never complete, un-ignore to execute in the IDE")
  @Test
  public void run() throws Exception {
    Harness harness = new Harness().withKryoMessageSerializer()
        .withSupplyingIngress(FGraphConstants.REQUEST_INGRESS, new MessageGenerator())
        .withPrintingEgress(FGraphConstants.RESULT_EGRESS);

    harness.start();
  }

  /** generate a random message, once a second a second. */
  private static final class MessageGenerator implements SerializableSupplier<FGraphMessages.InputMessage> {

    private static final long serialVersionUID = 1;

    // private CsvToBean<MyMessages.MyInputMessage> csvToBean = null;

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

      String line="";
      try {
        Thread.sleep(1_000);
        if ((line = br.readLine()) != null) {
          return newMessage(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      return newMessage(line);
    }

    private FGraphMessages.InputMessage newMessage(String line) {
      String[] strings = line.split("\t");
      if (line.length() < 5) {
        return new FGraphMessages.InputMessage("null", "null");
      }

      return new FGraphMessages.InputMessage(strings[2], strings[3]);
    }
  }
}
