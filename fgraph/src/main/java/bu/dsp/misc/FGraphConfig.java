package bu.dsp.misc;

public class FGraphConfig {
    private static int parallelism = 1;

    public static void setParallelism(int p) {
        FGraphConfig.parallelism = p;
    }

    public static int getParallelism() {
        return FGraphConfig.parallelism;
    }
}