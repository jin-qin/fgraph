package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.io.Router;

import bu.dsp.fgraph.FGraphMessages.InputMessage;

final class FGraphRouter implements Router<InputMessage> {
  private int parallelism = -1;

  public void setParallelism(Integer p) {
    this.parallelism = p;
  }

  @Override
  public void route(InputMessage message, Downstream<InputMessage> downstream) {
    String function_id = String.format("%d", message.getSrcId().hashCode() % parallelism);
    
    downstream.forward(FGraphConstants.FGRAPH_FUNCTION_TYPE, function_id, message);
  }
}