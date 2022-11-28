import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class pm_report_mapper extends Mapper<Object,
        Text, Text, LongWritable> {

    @Override
    public void map(Object key, Text value,
                    Context context) throws IOException,
            InterruptedException {

        // input data format => movie_name
        // no_of_views (tab separated)
        // we split the input data
        String[] tokens = value.toString().split(",");

        String full_name = tokens[0];
        long runs = Long.parseLong(tokens[1]);

        context.write(new Text(full_name),new LongWritable(runs));

    }


}