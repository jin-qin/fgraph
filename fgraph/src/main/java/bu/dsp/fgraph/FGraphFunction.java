package bu.dsp.fgraph;

import java.time.Duration;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedTable;

import bu.dsp.fgraph.FGraphMessages.EdgeMessage;
import bu.dsp.fgraph.FGraphMessages.OutputMessage;
import bu.dsp.fgraph.FGraphTypes.Vertex;

final class FGraphFunction implements StatefulFunction {  
  // PersistedTable<srcId, neighbors-sorted-by-timestamp>
  @Persisted
  PersistedTable<String, TreeSet<Vertex>> relationsTable = PersistedTable.of(
      "relations-table", 
      String.class, 
      TypeInformation.of(new TypeHint<TreeSet<Vertex>>(){}).getTypeClass(), 
      Expiration.expireAfterWriting(Duration.ofHours(1)));

  @Override
  public void invoke(Context context, Object input) {
    if (!(input instanceof EdgeMessage)) {
      throw new IllegalArgumentException("Unknown message received " + input);
    }
    EdgeMessage in = (EdgeMessage) input;

    // get neighbors of the vertex from the table
    TreeSet<Vertex> nbs = getNbsFromTable(in.getSrcId());
    
    // if exists, return false and do nothing
    nbs.add(new Vertex(in.getSrcId(), in.getDstId(), in.getTimestamp()));

    // update the table
    relationsTable.set(in.getSrcId(), nbs);

    OutputMessage out = new OutputMessage(in.getSrcId(), in.getDstId(), in.getTimestamp());

    context.send(FGraphConstants.RESULT_EGRESS, out);
  }

  /**
   * Get neighbors treeset from table
   */
  @Nonnull
  private TreeSet<Vertex> getNbsFromTable(String key) {
    TreeSet<Vertex> nbs = null;
    if (relationsTable.get(key) == null) {
      nbs = new TreeSet<Vertex>(new FGraphTypes.Vertex.NeighborComparator());
      relationsTable.set(key, nbs);
      return nbs;
    }

    return relationsTable.get(key);
  }
}