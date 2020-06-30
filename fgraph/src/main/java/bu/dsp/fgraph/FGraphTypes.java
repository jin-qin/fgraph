package bu.dsp.fgraph;

import java.util.Comparator;

public class FGraphTypes {
    static class NeighborOfVertex {
      static class NeighborComparator implements Comparator<NeighborOfVertex> {

        @Override
        public int compare(NeighborOfVertex nb1, NeighborOfVertex nb2) {
          Long ts1 = nb1.getTimestamp();
          Long ts2 = nb2.getTimestamp();
          Long id1 = Long.parseLong(nb1.getDstId());
          Long id2 = Long.parseLong(nb2.getDstId());

          if (ts1 == ts2) {
            return id1 > id2 ? 1 : (id1 < id2 ? -1 : 0);
          }

          return ts1 > ts2 ? 1 : (ts1 < ts2 ? -1 : 0);
        }
      }
      
      private String srcId; // source id of this vertex
      private String dstId; // vertex id
      private Long timestamp;
      
      public NeighborOfVertex(String srcId, String dstId, Long timestamp) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.timestamp = timestamp;
      }
  
      public String getSrcId() {
        return this.srcId;
      }
  
      public String getDstId() {
        return this.dstId;
      }
  
      public Long getTimestamp() {
        return this.timestamp;
      }
    }
}