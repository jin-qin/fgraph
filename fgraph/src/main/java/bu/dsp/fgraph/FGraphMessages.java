package bu.dsp.fgraph;

final class FGraphMessages {

    static final class InputMessage {
      private final String srcId;
      private final String dstId;
      private final Long timestamp;
  
      InputMessage(String srcId, String dstId, Long timestamp) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.timestamp = timestamp;
      }
  
      String getSrcId() {
        return srcId;
      }
  
      String getDstId() {
        return dstId;
      }

      Long getTimestamp() {
        return timestamp;
      }
    }
  
    static final class OutputMessage {
      private final String srcId;
      private final String dstId;
      private final Long timestamp;
  
      OutputMessage(String srcId, String dstId, Long timestamp) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.timestamp = timestamp;
      }
  
      String getSrcId() {
        return srcId;
      }
  
      String getDstId() {
        return dstId;
      }

      Long getTimestamp() {
        return timestamp;
      }
  
      @Override
      public String toString() {
        return String.format("OutputMessage(%s, %s, %d)", getSrcId(), getDstId(), getTimestamp());
      }
    }
  }
  