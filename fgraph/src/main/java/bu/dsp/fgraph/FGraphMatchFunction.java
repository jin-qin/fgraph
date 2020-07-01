package bu.dsp.fgraph;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
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
import bu.dsp.fgraph.FGraphMessages.OutputNeighborsMessage;
import bu.dsp.fgraph.FGraphMessages.QueryMessage;
import bu.dsp.fgraph.FGraphTypes.Vertex;
import bu.dsp.fgraph.FGraphTypes.QueryNeighborsArgs;

public class FGraphMatchFunction extends StatefulMatchFunction {

    // PersistedTable<srcId, neighbors-sorted-by-timestamp>
    @Persisted
    PersistedTable<String, TreeSet<Vertex>> relationsTable = PersistedTable.of(
        "relations-table", 
        String.class, 
        TypeInformation.of(new TypeHint<TreeSet<Vertex>>(){}).getTypeClass(), 
        Expiration.expireAfterWriting(Duration.ofHours(1)));
        
    @Override
    public void configure(MatchBinder binder) {
        binder.predicate(EdgeMessage.class, this::handleEdgeMessage)
              .predicate(QueryMessage.class, this::handleQueryMessage);
    }
    
    private void handleEdgeMessage(Context context, EdgeMessage msg) {
        // get neighbors of the vertex from the table
        TreeSet<Vertex> nbs = getNbsFromTable(msg.getSrcId());
        
        // if exists, return false and do nothing
        nbs.add(new Vertex(msg.getSrcId(), msg.getDstId(), msg.getTimestamp()));

        // update the table
        relationsTable.set(msg.getSrcId(), nbs);
    }

    private void handleQueryMessage(Context context, QueryMessage msg) {
        switch (msg.getCmd()) {
            case QUERY_SHORTEST_PATH:
                break;
            case QUERY_NEIGHBORS:
                retrieveSpecificNeighbors(context, msg);
                break;
            default:
                System.out.println("ERROR: Unknown query command!");
                break;
        }
    }

    /**
     * retrieve specific neighbors by hops and timestamp
     * @param msg
     */
    private void retrieveSpecificNeighbors(Context context, QueryMessage msg) {
        QueryNeighborsArgs args = (QueryNeighborsArgs)(msg.getMsgContent());
        String srcId = args.getSrcId();
        Integer nHop = args.getNumberOfHops();
        Long tsStart = args.getTimestampStart();
        Long tsEnd = args.getTimestampEnd();

        ArrayList<ArrayList<Vertex>> results = new ArrayList<ArrayList<Vertex>>();

        Deque<String> srcIdQueue = new LinkedList<String>();
        srcIdQueue.addLast(srcId);
        
        for (int i = 0; i < nHop; i++) {
            ArrayList<Vertex> nbsByHop = new ArrayList<>();
            results.add(nbsByHop);

            while (!srcIdQueue.isEmpty()) {
                String key = srcIdQueue.removeFirst();

                TreeSet<Vertex> nbs = getNbsFromTable(key);
                for (Vertex v : nbs) {
                    if (v.getTimestamp() >= tsStart && v.getTimestamp() < tsEnd) {
                        nbsByHop.add(v);
                    }
                }
            }

            for (Vertex v : nbsByHop) {
                srcIdQueue.add(v.getID());
            }

            results.set(i, nbsByHop);
        }

        OutputNeighborsMessage out = new OutputNeighborsMessage(msg.getSrcId(), tsStart, tsEnd, results);
        context.send(FGraphConstants.RESULT_EGRESS_QUERY_NEIGHBORS, out);
    }

    /**
     * Get neighbors treeset from table
     * @param key
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