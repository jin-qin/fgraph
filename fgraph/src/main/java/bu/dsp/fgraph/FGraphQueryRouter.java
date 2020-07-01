package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.io.Router;

import bu.dsp.fgraph.FGraphMessages.EdgeMessage;
import bu.dsp.fgraph.FGraphMessages.QueryMessage;;

final class FGraphQueryRouter implements Router<QueryMessage> {
  private int parallelism = -1;

  public void setParallelism(Integer p) {
    this.parallelism = p;
  }

  @Override
  public void route(QueryMessage message, Downstream<QueryMessage> downstream) {
    String function_id = String.format("%d", ((EdgeMessage)message.getMsgContent()).getSrcId().hashCode() % parallelism);

    downstream.forward(FGraphConstants.FGRAPH_FUNCTION_TYPE, function_id, message);
  }
}