package bu.dsp.fgraph;

import java.util.Collection;

import com.google.common.collect.Iterables;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;

import bu.dsp.fgraph.FGraphMessages.InputMessage;
import bu.dsp.fgraph.FGraphMessages.OutputMessage;

final class FGraphFunction implements StatefulFunction {
  @Persisted
  PersistedAppendingBuffer<InputMessage> relationsBuffer = PersistedAppendingBuffer.of("relations-buffer", InputMessage.class);

  @Override
  public void invoke(Context context, Object input) {
    if (!(input instanceof InputMessage)) {
      throw new IllegalArgumentException("Unknown message received " + input);
    }
    InputMessage in = (InputMessage) input;
    relationsBuffer.append(in);

    Integer accCount = sizeOfBuffer(relationsBuffer);

    OutputMessage out = new OutputMessage(in.getSrcId(), in.getDstId(), accCount);

    context.send(FGraphConstants.RESULT_EGRESS, out);
  }

  private Integer sizeOfBuffer(PersistedAppendingBuffer<InputMessage> buffer) {
    Iterable<InputMessage> data = buffer.view();
    // if (data instanceof Collection) {
    //   return ((Collection<?>) data).size();
    // }
    // return null;

    return Iterables.size(data);
  }
}