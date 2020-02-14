
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.hadoop.examples.HDFSAPI;

public class Step3 {
    // This represents the item IDs that are for the user (in this case myself, user ID 838)
    static HashSet<Integer> itemIDsForUser = new HashSet<Integer>();

    public static class Step31_ScoreMatrixProcessingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(LongWritable key, Text values, Context context)
                throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            int userID = Integer.parseInt(tokens[0]);

            // Last 3 digits of my student ID is 838
            if(userID == 838) {
                for(int i = 1; i < tokens.length; i++) {
                    String[] itemIDPref = tokens[i].split(":");
                    int itemID = Integer.parseInt(itemIDPref[0]);
                    itemIDsForUser.add(itemID);
                    String pref = itemIDPref[1];
                    k.set(itemID);
                    v.set(userID + ":" + pref);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step32_CooccurrenceMatrixProcessingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(LongWritable key, Text values,Context context)
                throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            String itemIDs = tokens[0];
            String frequency = tokens[1];
            int itemID1 = Integer.parseInt(itemIDs.split(":")[0]);
            int itemID2 = Integer.parseInt(itemIDs.split(":")[1]);
            k.set(itemID1);
            v.set(itemID2 + "=" + frequency);
            context.write(k, v);
        }
    }

    public static class Step3_MatrixMultiplicationReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<Integer, Integer> itemIDsFrequenciesMap = new HashMap<Integer, Integer>();
            HashMap<Integer, Double> userIDsPrefsMap = new HashMap<Integer, Double>();

            for(Text value: values) {
                if(value.toString().contains("=")) {
                    String[] itemIDFrequency = value.toString().split("=");
                    int itemID = Integer.parseInt(itemIDFrequency[0]);
                    int frequency = Integer.parseInt(itemIDFrequency[1]);
                    itemIDsFrequenciesMap.put(itemID, frequency);
                } else {
                    String[] userIDPref = value.toString().split(":");
                    int userID = Integer.parseInt(userIDPref[0]);
                    double pref = Double.parseDouble(userIDPref[1]);
                    userIDsPrefsMap.put(userID, pref);
                }
            }

            for(Map.Entry<Integer, Integer> entry: itemIDsFrequenciesMap.entrySet()){
                int itemID = entry.getKey();
                if(!itemIDsForUser.contains(itemID)) {
                    continue;
                }
                int frequency = entry.getValue();

                for (Map.Entry<Integer, Double> entry2: userIDsPrefsMap.entrySet()){
                    int userID = entry2.getKey();
                    double pref = entry2.getValue();
                    double result = frequency * pref;
                    context.write(new Text(userID + ":" + itemID), new DoubleWritable(result));
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException,
            ClassNotFoundException, InterruptedException {
        //get configuration info
        Configuration conf = Recommend.config();
        //get I/O path
        Path input1 = new Path(path.get("Step3Input1"));
        Path input2 = new Path(path.get("Step3Input2"));
        Path output = new Path(path.get("Step3Output"));
        // delete the last saved output
        /*
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        */
        // set job
        Job job =Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);

        //ChainMapper.addMapper(job, Step31_ScoreMatrixProcessingMapper.class, LongWritable.class, Text.class,
                //IntWritable.class, Text.class, conf);
        //ChainMapper.addMapper(job, Step32_CooccurrenceMatrixProcessingMapper.class, LongWritable.class, Text.class,
                //IntWritable.class, Text.class, conf);

        //job.setMapperClass(Step31_ScoreMatrixProcessingMapper.class);
        //job.setMapperClass(Step32_CooccurrenceMatrixProcessingMapper.class);

        MultipleInputs.addInputPath(job, input1, TextInputFormat.class,
                Step31_ScoreMatrixProcessingMapper.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class,
                Step32_CooccurrenceMatrixProcessingMapper.class);

        job.setReducerClass(Step3_MatrixMultiplicationReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, output);
        // run job
        job.waitForCompletion(true);
    }
}

