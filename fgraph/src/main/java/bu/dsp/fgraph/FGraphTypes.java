package bu.dsp.fgraph;

import java.util.Comparator;

public class FGraphTypes {
    static class Vertex {
      static class NeighborComparator implements Comparator<Vertex> {

        @Override
        public int compare(Vertex nb1, Vertex nb2) {
          Long ts1 = nb1.getTimestamp();
          Long ts2 = nb2.getTimestamp();
          Long id1 = Long.parseLong(nb1.getID());
          Long id2 = Long.parseLong(nb2.getID());

          if (ts1 == ts2) {
            return id1 > id2 ? 1 : (id1 < id2 ? -1 : 0);
          }

          return ts1 > ts2 ? 1 : (ts1 < ts2 ? -1 : 0);
        }
      }
      
      private String srcId; // source id of this vertex
      private String ID; // vertex id
      private Long timestamp;
      
      public Vertex(String srcId, String ID, Long timestamp) {
        this.srcId = srcId;
        this.ID = ID;
        this.timestamp = timestamp;
      }
  
      public String getSrcId() {
        return this.srcId;
      }
  
      public String getID() {
        return this.ID;
      }
  
      public Long getTimestamp() {
        return this.timestamp;
      }
    }

    static class QueryNeighborsArgs {
      private String srcId;
      private Integer nHop; // query how many hops neighbors?
      private Long timestampStart;
      private Long timestampEnd;

      public QueryNeighborsArgs(String srcId, Integer nHop, Long timestampStart, Long timestampEnd) {
        this.srcId = srcId;
        this.nHop = nHop;
        this.timestampStart = timestampStart;
        this.timestampEnd = timestampEnd;
      }

      String getSrcId() {
        return srcId;
      }

      Integer getNumberOfHops() {
        return nHop;
      }

      Long getTimestampStart() {
        return timestampStart;
      }

      Long getTimestampEnd() {
        return timestampEnd;
      }
    }
}