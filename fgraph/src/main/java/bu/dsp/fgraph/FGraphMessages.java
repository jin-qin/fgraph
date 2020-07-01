package bu.dsp.fgraph;

final class FGraphMessages {

    static final class EdgeMessage {
      private final String srcId;
      private final String dstId;
      private final Long timestamp;
  
      EdgeMessage(String srcId, String dstId, Long timestamp) {
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

    static final class QueryMessage {
      public enum Command {
        QUERY_SHORTEST_PATH
      }

      private final Command cmd; // query command
      private final Object msgContent;

      QueryMessage(Command cmd, Object content) {
        this.cmd = cmd;
        this.msgContent = content;
      }

      Command getCmd() {
        return cmd;
      }

      Object getMsgContent() {
        return this.msgContent;
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
  