import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HBaseBulkLoad {
    // Class to implement the mapper interface
    static class CustomMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
    {
        // Map interface of the MapReduce job
        public void map(LongWritable key, Writable value, Context context) throws IOException, InterruptedException {

            Configuration config = context.getConfiguration();
            String family = "test";
            String column = "testColumn";
            String sKey = String.valueOf((new Random()).nextLong());
            byte[] bKey = Bytes.toBytes(sKey);
            Put put = new Put(bKey);
            put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value.toString()));

            ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
            context.write(ibKey, put);
        }
    }
    // Main method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage - HBaseBulkLoad <host> <table_name>  <input-file-directory>");
            System.exit(-1);
        }

        // Create a job for the map-reduce task
        Job job = new Job();
        job.setJarByClass(HBaseBulkLoad.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);

        // set the mapper and reducer class
        job.setMapperClass(CustomMapper.class);
        // job.setReducerClass(CustomReducer.class);

        // Set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        getFilePaths(args[0]);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        // Set the input and output path
        //FileOutputFormat.setOutputPath(job, new Path(args[3]));

        String host = args[0];
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", host);
        HTable hTable = new HTable(conf , args[1]);
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        // Wait for the job to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void getFilePaths(String arg) {
        File dir = new File(arg);
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            List<String> fileList = new ArrayList<String>();
            for (File file : files)
                fileList.add(file.getAbsolutePath());

        }
    }

/*
    // Class to implement the reducer interface
    static class CustomReducer extends Reducer<Text, Text, Text, Text> {
        // Reduce interface of the MapReduce job
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Vars to hold the highest values
            float max_rating = Float.MIN_VALUE;
            int max_votes = Integer.MIN_VALUE;
            String[] line_values;
            String output = "";
            int vote;
            float rating;

            // Iterate through each value
            for (Text value : values) {
                // value is of the format <[Total Votes] [Rating] [Movie]>
                line_values = value.toString().split("\t");

                // Get the numerical values of a votes & a ratings
                rating = Float.parseFloat(line_values[1]);
                vote = Integer.parseInt(line_values[0]);

                // Check if the current rating is more than max_rating
                if (rating > max_rating) {
                    max_rating = rating;
                    max_votes = vote;

                    output = line_values[2] + "\t" + line_values[1];
                } else if (rating == max_rating) {
                    // In case of tie, decide by the number of votes
                    if (vote > max_votes) {
                        max_rating = rating;
                        max_votes = vote;

                        output = line_values[2] + "\t" + line_values[1];
                    }
                }
            }
            // Write the output
            context.write(key, new Text(output));
        }
    }*/
}

