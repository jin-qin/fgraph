package bu.dsp.fgraph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Set;

import bu.dsp.fgraph.FGraphMessages.SyncMessage;

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
      private Integer distance; // the distance to the source vertex, default is INF
      private Boolean visited;  // visited or not, default is false
      
      public Vertex(String srcId, String ID, Long timestamp) {
        this.srcId = srcId;
        this.ID = ID;
        this.timestamp = timestamp;
        this.distance = Integer.MAX_VALUE;
        this.visited = false;
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

      public void setDistance(Integer distance) {
        this.distance = distance;
      }

      public Integer getDistance() {
        return this.distance;
      }

      public void setVisited(Boolean visited) {
        this.visited = visited;
      }

      public Boolean getVisited() {
        return this.visited;
      }
    }

    static class QueryArgsBase {
      protected String srcId;
      protected Integer nHop; // query how many hops neighbors?
      protected Long timestampStart;
      protected Long timestampEnd;

      public QueryArgsBase(String srcId, Integer nHop, Long timestampStart, Long timestampEnd) {
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

    static class QueryNeighborsArgs extends QueryArgsBase{
      public QueryNeighborsArgs(String srcId, Integer nHop, Long timestampStart, Long timestampEnd) {
        super(srcId, nHop, timestampStart, timestampEnd);
      }
    }

    static class QueryShortestPathArgs extends QueryArgsBase{
      private String dstId;

      public QueryShortestPathArgs(String srcId, String dstId, Integer nHop, Long timestampStart, Long timestampEnd) {
        super(srcId, nHop, timestampStart, timestampEnd);

        this.dstId = dstId;
      }

      String getDstId() {
        return dstId;
      }
    }

    static class SyncMessageArgsBase {
      protected Set<String> nbsIds; // neighbors ids that need to be requested
      protected int hopsToGo; // how many hops remain.
      protected Long tsStart;
      protected Long tsEnd;

      public SyncMessageArgsBase(Set<String> nbsIds, int hopsToGo, Long tsStart, Long tsEnd) {
        this.nbsIds = nbsIds;
        this.hopsToGo= hopsToGo;
        this.tsStart = tsStart;
        this.tsEnd = tsEnd;
      }

      Set<String> getNbsIds() {
        return nbsIds;
      }

      int getCurrentHop() {
        return hopsToGo;
      }

      void addNbId(String nbId) {
        nbsIds.add(nbId);
      }
    }

    static class SyncNeighborsArgs extends SyncMessageArgsBase{
      private String srcId; // source vertex id.
      private String dstId; // destination vertex id, i.e. the target vertex in the shortest path query.

      public SyncNeighborsArgs(Set<String> nbsIds, int hopsToGo, String dstId, Long tsStart, Long tsEnd) {
        super(nbsIds, hopsToGo, tsStart, tsEnd);

        this.dstId = dstId;
      }

      String getDstId() {
        return dstId;
      }
    }
}