import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Main <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path input        = new Path(args[0]);
        Path cleanedOutput = new Path(args[1] + "/cleaned");
        Path finalOutput  = new Path(args[1] + "/frequent");

        // Remove output dirs if they exist
        if (fs.exists(cleanedOutput)) fs.delete(cleanedOutput, true);
        if (fs.exists(finalOutput))   fs.delete(finalOutput, true);

        // ── STEP 1: Preprocess ──────────────────────────────────────
        System.out.println("=== STEP 1: Preprocessing dataset ===");
        long startTime = System.currentTimeMillis();

        Job preprocessJob = DataPreprocessor.createJob(conf, input, cleanedOutput);
        if (!preprocessJob.waitForCompletion(true)) {
            System.err.println("Preprocessing failed!");
            System.exit(1);
        }

        long prepTime = System.currentTimeMillis() - startTime;
        System.out.println("Preprocessing done in " + prepTime + "ms");

        // ── STEP 2: Frequency Analysis ──────────────────────────────
        System.out.println("=== STEP 2: Running Frequency Analysis ===");
        long freqStart = System.currentTimeMillis();

        Job freqJob = FrequencyAnalyzer.createJob(conf, cleanedOutput, finalOutput);
        if (!freqJob.waitForCompletion(true)) {
            System.err.println("Frequency analysis failed!");
            System.exit(1);
        }

        long freqTime = System.currentTimeMillis() - freqStart;
        long totalTime = System.currentTimeMillis() - startTime;

        System.out.println("Frequency analysis done in " + freqTime + "ms");
        System.out.println("=== TOTAL TIME: " + totalTime + "ms ===");
        System.out.println("Results saved to: " + finalOutput);
        System.out.println("  Singles: " + finalOutput + "/part-r-00000");
        System.out.println("  Pairs:   " + finalOutput + "/part-r-00001");
    }
}