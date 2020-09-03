package bu.dsp.fgraph;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
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
import bu.dsp.fgraph.FGraphMessages.OutputNeighborsMessage;
import bu.dsp.fgraph.FGraphMessages.QueryMessage;
import bu.dsp.fgraph.FGraphMessages.SyncMessage;
import bu.dsp.fgraph.FGraphMessages.SyncMessage.Command;
import bu.dsp.fgraph.FGraphTypes.Vertex;
import bu.dsp.misc.FGraphUtils;
import bu.dsp.fgraph.FGraphTypes.QueryNeighborsArgs;
import bu.dsp.fgraph.FGraphTypes.QueryShortestPathArgs;
import bu.dsp.fgraph.FGraphTypes.SyncNeighborsArgs;

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
              .predicate(QueryMessage.class, this::handleQueryMessage)
              .predicate(SyncMessage.class, this::handleSyncMessage);
    }
    
    private void handleEdgeMessage(Context context, EdgeMessage msg) {
        // get neighbors of the vertex from the table
        TreeSet<Vertex> nbs = getNbsFromTable(msg.getSrcId());
        
        // if exists, return false and do nothing
        Vertex new_nb = new Vertex(msg.getSrcId(), msg.getDstId(), msg.getTimestamp());
        new_nb.setDistance(msg.getDistance());
        nbs.add(new_nb);

        // update the table
        relationsTable.set(msg.getSrcId(), nbs);
    }

    private void handleQueryMessage(Context context, QueryMessage msg) {
        switch (msg.getCmd()) {
            case QUERY_SHORTEST_PATH:
                onQueryShortestPath(context, msg);
                break;
            case QUERY_NEIGHBORS:
                onQueryNeighbors(context, msg);
                break;
            default:
                System.out.println("ERROR: Unknown query command!");
                break;
        }
    }

    private void handleSyncMessage(Context context, SyncMessage msg) {
        switch (msg.getCmd()) {
            case SYNC_NEIGHBORS_REQ:
                onSyncNeighborsReq(context, msg);
                break;
            case SYNC_NEIGHBORS_RSP:
                onSyncNeighborsRsp(context, msg);
                break;
            default:
                System.out.println("ERROR: Unknown query command!");
                break;
        }
    }

    private void onQueryShortestPath(Context context, QueryMessage msg) {
        QueryShortestPathArgs args = (QueryShortestPathArgs)(msg.getMsgContent());
        String srcId = args.getSrcId();
        String dstId = args.getDstId();
        Integer nHop = args.getNumberOfHops();
        Long tsStart = args.getTimestampStart();
        Long tsEnd = args.getTimestampEnd();

        Integer partition_src = FGraphUtils.getPartition(srcId);
        String function_id_src = partition_src.toString();

        HashMap<String, SyncNeighborsArgs> nbsToReqSync = new HashMap<>();

        // first, search the destination vertex to see if the source vertex can connect it in a specific hop number.
        // if we found one neighbor which is not in this partition, suspend searching and send a message to ask the neighbor's partition to sync its neighbors.

        ArrayList<ArrayList<Vertex>> nbs = retrieveSpecificNeighbors(srcId, 1, tsStart, tsEnd);
        ArrayList<Vertex> nbs_first_hop = nbs.get(0);
        for (Vertex nb : nbs_first_hop) {
            // validate timestamp
            Long ts = nb.getTimestamp();
            if (ts < tsStart || ts > tsEnd) continue;

            // nb.setVisited(true);
            
            // if this neighbor is not in this partition and has never been synced, suspend searching and send a message to sync
            Integer partition_nb = FGraphUtils.getPartition(nb.getID());
            if (partition_nb != partition_src) {
                // check if we can find the neighbors of this neighbor, i.e. already synced this neighbor's neighbors
                TreeSet<Vertex> second_nbs = getNbsFromTable(nb.getID());
                if (second_nbs.size() > 0) continue;

                String function_id_nb = partition_nb.toString();
                
                // check to add neighbor that needs to be requested
                if (!nbsToReqSync.containsKey(function_id_nb)) {
                    Set<String> nbsToAdd = new HashSet<>();
                    nbsToAdd.add(nb.getID());
                    nbsToReqSync.put(function_id_nb, new SyncNeighborsArgs(nbsToAdd, nHop, dstId, tsStart, tsEnd));
                    continue;
                }

                // if exists, update neighbors
                SyncNeighborsArgs syncNbsArgs = nbsToReqSync.get(function_id_nb);
                syncNbsArgs.addNbId(nb.getID());
                nbsToReqSync.put(function_id_nb, syncNbsArgs);
            }
        }


        if (nbsToReqSync.size() <= 0) return;

        for (String func_id_nb : nbsToReqSync.keySet()) {
            context.send(FGraphConstants.FGRAPH_FUNCTION_TYPE, 
                        func_id_nb, 
                        new SyncMessage(function_id_src, Command.SYNC_NEIGHBORS_REQ, nbsToReqSync.get(func_id_nb)));
        }
    }

    private void onQueryNeighbors(Context context, QueryMessage msg) {
        QueryNeighborsArgs args = (QueryNeighborsArgs)(msg.getMsgContent());
        String srcId = args.getSrcId();
        Integer nHop = args.getNumberOfHops();
        Long tsStart = args.getTimestampStart();
        Long tsEnd = args.getTimestampEnd();

        ArrayList<ArrayList<Vertex>> nbs = retrieveSpecificNeighbors(srcId, nHop, tsStart, tsEnd);

        OutputNeighborsMessage out = new OutputNeighborsMessage(msg.getSrcId(), tsStart, tsEnd, nbs);
        context.send(FGraphConstants.RESULT_EGRESS_QUERY_NEIGHBORS, out);
    }

    private void onSyncNeighborsReq(Context context, SyncMessage msg) {
        SyncNeighborsArgs args = (SyncNeighborsArgs) msg.getMsgContent();
        if (args.hopsToGo <=0 ) return;
        
        Set<String> nbsToSync = new HashSet<>();
        SyncNeighborsArgs argsReply = new SyncNeighborsArgs(nbsToSync, args.hopsToGo-1, args.getDstId(), args.tsStart, args.tsEnd);

        for (String nbId : args.getNbsIds()) {
            retrieveSpecificNeighbors(nbId, 1, args.tsStart, args.tsEnd);
            argsReply.addNbId(nbId);
        }

        // SyncMessage msgReply = new SyncMessage(...);
        // context.reply(msgReply);
    }

    private void onSyncNeighborsRsp(Context context, SyncMessage msg) {

    }


    /**
     * retrieve specific neighbors by hops and timestamp
     * @param msg
     */
    private ArrayList<ArrayList<Vertex>> retrieveSpecificNeighbors(String srcId, Integer nHop, Long tsStart, Long tsEnd) {
        ArrayList<ArrayList<Vertex>> results = new ArrayList<ArrayList<Vertex>>();

        Deque<String> srcIdQueue = new LinkedList<String>();
        srcIdQueue.addLast(srcId);
        
        for (int i = 0; i < nHop; i++) {
            ArrayList<Vertex> nbsByHop = new ArrayList<>();

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

            results.add(nbsByHop);
        }

        return results;
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