package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

import bu.dsp.fgraph.FGraphMessages.InputMessage;
import bu.dsp.fgraph.FGraphMessages.OutputMessage;

final class FGraphConstants {
  static final IngressIdentifier<InputMessage> REQUEST_INGRESS =
      new IngressIdentifier<>(
          InputMessage.class, "bu.dsp.fgraph", "in");

  static final EgressIdentifier<OutputMessage> RESULT_EGRESS =
      new EgressIdentifier<>(
          "bu.dsp.fgraph", "out", OutputMessage.class);

  static final FunctionType FGRAPH_FUNCTION_TYPE =
      new FunctionType("bu.dsp.fgraph", "fgraph-function");
}