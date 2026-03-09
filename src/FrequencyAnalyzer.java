import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FrequencyAnalyzer {

    public static class FreqMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            StringTokenizer itr = new StringTokenizer(line);
            List<String> words = new ArrayList<>();
            while (itr.hasMoreTokens()) {
                words.add(itr.nextToken());
            }

            for (String word : words) {
                context.write(new Text("s:" + word), one);
            }

            for (int i = 0; i < words.size() - 1; i++) {
                String pair = words.get(i) + " " + words.get(i + 1);
                context.write(new Text("p:" + pair), one);
            }
        }
    }

    public static class FreqCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static class FreqPartitioner
            extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (numPartitions == 1) return 0;
            return key.toString().startsWith("s:") ? 0 : 1 % numPartitions;
        }
    }

    public static class FreqReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static final int min = 5000;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            if (sum >= min) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static Job createJob(Configuration conf, Path input, Path output)
            throws Exception {

        Job job = Job.getInstance(conf, "frequency analyzer");

        job.setJarByClass(FrequencyAnalyzer.class);
        job.setMapperClass(FreqMapper.class);
        job.setCombinerClass(FreqCombiner.class);
        job.setPartitionerClass(FreqPartitioner.class);
        job.setReducerClass(FreqReducer.class);

        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}