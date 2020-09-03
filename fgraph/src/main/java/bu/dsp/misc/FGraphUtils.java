package bu.dsp.misc;

public class FGraphUtils {

    /**
     * 
     * @param key, currently, key is the source id of a vertex
     * @return
     */
    public static int getPartition(String key) {
        return key.hashCode() % FGraphConfig.getParallelism();
    }
}