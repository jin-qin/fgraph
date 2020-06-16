package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.io.Router;

import bu.dsp.fgraph.FGraphMessages.InputMessage;

final class FGraphRouter implements Router<InputMessage> {

  @Override
  public void route(InputMessage message, Downstream<InputMessage> downstream) {
    downstream.forward(FGraphConstants.FGRAPH_FUNCTION_TYPE, message.getSrcId(), message);
  }
}