package bu.dsp.fgraph;

final class FGraphMessages {

    static final class InputMessage {
      private final String srcId;
      private final String dstId;
  
      InputMessage(String srcId, String dstId) {
        this.srcId = srcId;
        this.dstId = dstId;
      }
  
      String getSrcId() {
        return srcId;
      }
  
      String getDstId() {
        return dstId;
      }
    }
  
    static final class OutputMessage {
      private final String srcId;
      private final String dstId;
      private final Integer accCount; // accumulate message number
  
      OutputMessage(String srcId, String dstId, Integer accCount) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.accCount = accCount;
      }
  
      String getSrcId() {
        return srcId;
      }
  
      String getDstId() {
        return dstId;
      }

      Integer getAccCount() {
        return accCount;
      }
  
      @Override
      public String toString() {
        return String.format("OutputMessage(%s, %s, %d)", getSrcId(), getDstId(), getAccCount());
      }
    }
  }
  