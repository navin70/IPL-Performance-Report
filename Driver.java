import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;


public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{


        Configuration conf = new Configuration();
        // Create a new Job
        Job job1 = Job.getInstance(conf,"wordcount");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(pm_report_mapper.class);
        job1.setReducerClass(pm_report_reducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapOutputKeyClass(NullWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setInputFormatClass(TextInputFormat.class);

        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job1, outDir);
        job1.waitForCompletion(true);


        FileSystem fs = FileSystem.get(job1.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        // Submit the job, then poll for progress until the job is complete

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Rank");
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(rank_mapper.class);
        job2.setReducerClass(rank_reducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true)?0:1);


    }
}
