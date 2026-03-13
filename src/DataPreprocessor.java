import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataPreprocessor {

    public static class CleanMapper
            extends Mapper<Object, Text, Text, NullWritable> {

        private static final Set<String> palabrasFiltradas = new HashSet<>(Arrays.asList(
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
            "for", "of", "with", "by", "from", "is", "it", "its", "was",
            "are", "were", "be", "been", "being", "have", "has", "had",
            "do", "does", "did", "will", "would", "could", "should", "may",
            "might", "shall", "can", "that", "this", "these", "those",
            "i", "you", "he", "she", "we", "they", "me", "him", "her",
            "us", "them", "my", "your", "his", "our", "their", "what",
            "which", "who", "whom", "not", "no", "so", "if", "as", "up",
            "out", "about", "into", "than", "then", "when", "there", "s",
            "t", "re", "ve", "ll", "d", "m"
        ));

public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

    String json = value.toString();

    int start = json.indexOf("\"text\":\"");
    if (start == -1) return;

    json = json.substring(start + 8);
    int end = json.indexOf("\"");
    if (end == -1) return;

    String line = json.substring(0, end).trim();

    if (line.isEmpty()) return;

    line = line.toLowerCase();

    line = line.replaceAll("[^a-z0-9 ]", " ");

    line = line.trim().replaceAll("\\s+", " ");

    StringBuilder filtered = new StringBuilder();
    for (String p : line.split(" ")) {
        if (p.length() <= 1) continue;
        if (palabrasFiltradas.contains(p)) continue;
        if (p.matches(".*\\d.*")) continue;

        filtered.append(p).append(" ");
    }

    String result = filtered.toString().trim();

    if (result.length() < 3) return;

    context.write(new Text(result), NullWritable.get());
}}

    public static Job createJob(Configuration conf, Path input, Path output)
            throws Exception {
        Job job = Job.getInstance(conf, "data preprocessor");

        job.setJarByClass(DataPreprocessor.class);
        job.setMapperClass(CleanMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
