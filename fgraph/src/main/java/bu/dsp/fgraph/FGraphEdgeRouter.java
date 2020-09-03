package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.io.Router;

import bu.dsp.fgraph.FGraphMessages.EdgeMessage;
import bu.dsp.misc.FGraphUtils;

final class FGraphEdgeRouter implements Router<EdgeMessage> {
  @Override
  public void route(EdgeMessage message, Downstream<EdgeMessage> downstream) {
    Integer partition = FGraphUtils.getPartition(message.getSrcId());
    String function_id = String.format("%d", partition);

    downstream.forward(FGraphConstants.FGRAPH_FUNCTION_TYPE, function_id, message);
  }
}