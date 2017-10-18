import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;

public class Tp1{
public static class PivotMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long column = 0;
        long somethingLikeRow = key.get();
        /*réglage du splitter en semi-column*/
        for (String num : value.toString().split(";")) {
            context.write(new LongWritable(column), new Text(somethingLikeRow + "\t" + num));
            ++column;
        }
    }
}

public static class PivotReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeMap<Long, String> row = new TreeMap<Long, String>();
        for (Text text : values) {
            String[] parts = text.toString().split("\t");
            row.put(Long.valueOf(parts[0]), parts[1]);
        }
        String rowString = StringUtils.join(row.values(), ' ');
        context.write(key, new Text(rowString));
    }
}
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Tp1");
    job.setJarByClass(Tp1.class);
    job.setMapperClass(PivotMap.class);
    job.setReducerClass(PivotReduce.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

