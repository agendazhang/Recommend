import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import HDFSAPI;

public class Step5 {
    public static class Step5_SortRecommendationScoresMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            String userIDItemID = tokens[0];
            double score = Double.parseDouble(tokens[1]);
            context.write(new Text(userIDItemID), new DoubleWritable(score));

        }
    }

    public static class Step5_SortRecommendationScoresReducer extends Reducer<Text, DoubleWritable, IntWritable, DoubleWritable> {

        // Group each itemID and score as a 2-element tuple
        private class Pair implements Comparable<Pair> {
            private int itemID;
            private double score;

            private Pair(int itemID, double score) {
                this.itemID = itemID;
                this.score = score;
            }

            // Compares based on the score
            public int compareTo(Pair other) {
                if(this.score <= other.score) {
                    return -1;
                } else {
                    return 1;
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Pair pair = (Pair) o;
                return itemID == pair.itemID &&
                        score == pair.score;
            }

            @Override
            public int hashCode() {
                return Objects.hash(itemID, score);
            }
        }

        private TreeSet<Pair> treeSet;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            treeSet = new TreeSet<Pair>();
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            for (DoubleWritable score: values) {
                String userIDItemID = key.toString();
                int userID = Integer.parseInt(userIDItemID.split(":")[0]);

                int itemID = Integer.parseInt(userIDItemID.split(":")[1]);
                // Add the new Pair into the TreeSet, which will order them automatically
                treeSet.add(new Pair(itemID, score.get()));

            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Pair> iterator = treeSet.descendingIterator();

            // Output the sorted itemIDs based on recommendation score
            while(iterator.hasNext()) {
                Pair next = iterator.next();
                int itemID = next.itemID;
                double score = next.score;
                context.write(new IntWritable(itemID), new DoubleWritable(score));
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        //get configuration info
        Configuration conf = Recommend.config();
        // I/O path
        Path input = new Path(path.get("Step5Input"));
        Path output = new Path(path.get("Step5Output"));
        // delete last saved output
        /*
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        */
        // set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Step5_SortRecommendationScoresMapper.class);
        job.setReducerClass(Step5_SortRecommendationScoresReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}

