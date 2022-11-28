import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



public class pm_report_reducer extends Reducer<Text,
        LongWritable, Text, LongWritable> {

    LongWritable sum = new LongWritable();


    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {

        String name = key.toString();
        int count = 0;


        for (LongWritable run : values) {
            count += run.get();
        }

        sum.set(count);
        context.write(new Text(name),sum);


    }
}