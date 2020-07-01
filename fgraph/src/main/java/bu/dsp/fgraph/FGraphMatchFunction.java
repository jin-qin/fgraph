package bu.dsp.fgraph;

import java.util.TreeSet;

import javax.annotation.Nonnull;

import java.time.Duration;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.match.MatchBinder;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedTable;

import bu.dsp.fgraph.FGraphMessages.EdgeMessage;
import bu.dsp.fgraph.FGraphMessages.OutputMessage;
import bu.dsp.fgraph.FGraphMessages.QueryMessage;
import bu.dsp.fgraph.FGraphTypes.NeighborOfVertex;

public class FGraphMatchFunction extends StatefulMatchFunction {

    // PersistedTable<srcId, neighbors-sorted-by-timestamp>
    @Persisted
    PersistedTable<String, TreeSet<NeighborOfVertex>> relationsTable = PersistedTable.of(
        "relations-table", 
        String.class, 
        TypeInformation.of(new TypeHint<TreeSet<NeighborOfVertex>>(){}).getTypeClass(), 
        Expiration.expireAfterWriting(Duration.ofHours(1)));
        
    @Override
    public void configure(MatchBinder binder) {
        binder.predicate(EdgeMessage.class, this::handleEdgeMessage)
              .predicate(QueryMessage.class, this::handleQueryMessage);
    }
    
    private void handleEdgeMessage(Context context, EdgeMessage msg) {
        // get neighbors of the vertex from the table
        TreeSet<NeighborOfVertex> nbs = getNbsFromTable(msg.getSrcId());
        
        // if exists, return false and do nothing
        nbs.add(new NeighborOfVertex(msg.getSrcId(), msg.getDstId(), msg.getTimestamp()));

        // update the table
        relationsTable.set(msg.getSrcId(), nbs);
    }

    private void handleQueryMessage(Context context, QueryMessage msg) {
        switch (msg.getCmd()) {
            case QUERY_SHORTEST_PATH:
                break;
        
            default:
                System.out.println("ERROR: Unknown query command!");
                break;
        }
    }

    /**
     * Get neighbors treeset from table
     */
    @Nonnull
    private TreeSet<NeighborOfVertex> getNbsFromTable(String key) {
        TreeSet<NeighborOfVertex> nbs = null;
        if (relationsTable.get(key) == null) {
            nbs = new TreeSet<NeighborOfVertex>(new FGraphTypes.NeighborOfVertex.NeighborComparator());
            relationsTable.set(key, nbs);
            return nbs;
        }

        return relationsTable.get(key);
    }
}