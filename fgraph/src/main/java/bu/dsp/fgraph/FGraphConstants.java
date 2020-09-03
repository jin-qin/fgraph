package bu.dsp.fgraph;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

import bu.dsp.fgraph.FGraphMessages.EdgeMessage;
import bu.dsp.fgraph.FGraphMessages.OutputMessage;
import bu.dsp.fgraph.FGraphMessages.OutputNeighborsMessage;
import bu.dsp.fgraph.FGraphMessages.QueryMessage;

final class FGraphConstants {
    static final IngressIdentifier<EdgeMessage> REQUEST_INGRESS_EDEG = 
        new IngressIdentifier<>(EdgeMessage.class, "bu.dsp.fgraph", "edge");

    static final IngressIdentifier<QueryMessage> REQUEST_INGRESS_QUERY = 
        new IngressIdentifier<>(QueryMessage.class, "bu.dsp.fgraph", "query");

    static final EgressIdentifier<OutputNeighborsMessage> RESULT_EGRESS_QUERY_SHORTEST_PATH = 
        new EgressIdentifier<>("bu.dsp.fgraph", "out_query_shortest_path", OutputNeighborsMessage.class);

    static final EgressIdentifier<OutputNeighborsMessage> RESULT_EGRESS_QUERY_NEIGHBORS = 
        new EgressIdentifier<>("bu.dsp.fgraph", "out_query_neighbors", OutputNeighborsMessage.class);

    static final EgressIdentifier<OutputMessage> RESULT_EGRESS = 
        new EgressIdentifier<>("bu.dsp.fgraph", "out", OutputMessage.class);

    static final FunctionType FGRAPH_FUNCTION_TYPE = 
        new FunctionType("bu.dsp.fgraph", "fgraph-function");
}