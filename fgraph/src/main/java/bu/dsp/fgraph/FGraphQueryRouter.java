package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.io.Router;

import bu.dsp.fgraph.FGraphMessages.QueryMessage;
import bu.dsp.misc.FGraphUtils;;

final class FGraphQueryRouter implements Router<QueryMessage> {
  @Override
  public void route(QueryMessage message, Downstream<QueryMessage> downstream) {
    Integer partition = FGraphUtils.getPartition(message.getSrcId());
    String function_id = String.format("%d", partition);

    downstream.forward(FGraphConstants.FGRAPH_FUNCTION_TYPE, function_id, message);
  }
}