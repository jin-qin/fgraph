package bu.dsp.fgraph;

import java.util.ArrayList;

import bu.dsp.fgraph.FGraphTypes.Vertex;

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

      @Override
      public String toString() {
        return String.format("EdgeMessage(%s, %s, %d)", getSrcId(), getDstId(), getTimestamp());
      }
    }

    static final class QueryMessage {
      public enum Command {
        QUERY_SHORTEST_PATH,
        QUERY_NEIGHBORS
      }

      private final Command cmd; // query command
      private final String srcId; // the vertex to query.
      private final Object msgContent;

      QueryMessage(Command cmd, String srcId, Object content) {
        this.cmd = cmd;
        this.msgContent = content;
        this.srcId = srcId;
      }

      Command getCmd() {
        return cmd;
      }

      String getSrcId() {
        return this.srcId;
      }

      Object getMsgContent() {
        return this.msgContent;
      }

      @Override
      public String toString() {
        return String.format("QueryMessage(%s, %s)", cmd.toString(), msgContent.toString());
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

    static final class OutputNeighborsMessage {
      private final String srcId;
      private final Long timestampStart;
      private final Long timestampEnd;
      private final ArrayList<ArrayList<Vertex>> nbsByHop;

      OutputNeighborsMessage(String srcId, Long timestampStart, Long timestampEnd, ArrayList<ArrayList<Vertex>> nbsByHop) {
        this.srcId = srcId;
        this.timestampStart = timestampStart;
        this.timestampEnd = timestampEnd;
        this.nbsByHop = nbsByHop;
      }

      String getSrcId() {
        return srcId;
      }

      Long getTimestampStart() {
        return timestampStart;
      }

      Long getTimestampEnd() {
        return timestampEnd;
      }

      ArrayList<ArrayList<Vertex>> getNbsByHop() {
        return nbsByHop;
      }

      @Override
      public String toString() {
        return String.format("OutputNeighborsMessage(%s, %d, %d, %s)", getSrcId(), getTimestampStart(), getTimestampEnd(), nbsByHop.toString());
      }
    }
  }
  