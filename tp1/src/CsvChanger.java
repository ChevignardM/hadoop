import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Map.Entry;

public class CsvChanger {


		public static class PivotMap extends Mapper<LongWritable, Text, LongWritable, MapWritable> {

		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    
		    	//set du splitter ,
		    String[] s = value.toString().split(",");
		      int row = Integer.parseInt(s[0].trim());
		      String[] vals = s[1].trim().split("\\s+");
		      MapWritable map = new MapWritable();
		      int col = 0;
		      for(String v : vals) {
		        int val = Integer.parseInt(v);
		        map.put(new LongWritable(row), new IntWritable(val));
		        context.write(new LongWritable(col), map);
		        col++;
			
		      	}
		    }
		}
		
		public static class PivotReduce extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
			
			public void reduce(LongWritable key, Iterable<MapWritable> maps, Context context)
				      throws IOException, InterruptedException {
				        SortedMap<LongWritable,IntWritable> rowVals = new TreeMap<LongWritable,IntWritable>();
				        for (MapWritable map : maps) {
				          for(Entry<Writable, Writable>  entry : map.entrySet()) {
				            rowVals.put((LongWritable) entry.getKey(),(IntWritable) entry.getValue());
				          }
				        }

				        StringBuffer sb = new StringBuffer();
				        for(IntWritable rowVal : rowVals.values()) {
				          sb.append(rowVal.toString());
				          sb.append(" ");
				        }
				        context.write(key,new Text(sb.toString()));
				}
			
		}

	        
	        public static void main(String[] args) throws Exception {
	            Configuration conf = new Configuration();

	            Job job = new Job(conf, "pivot");
	            job.setJarByClass(CsvChanger.class);
	            job.setOutputKeyClass(LongWritable.class);
	            job.setOutputValueClass(MapWritable.class);
	            job.setMapperClass(PivotMap.class);
	            job.setReducerClass(PivotReduce.class);
	            job.setInputFormatClass(TextInputFormat.class);
	            job.setOutputFormatClass(TextOutputFormat.class);
	            FileInputFormat.addInputPath(job, new Path(args[0]));
	            FileOutputFormat.setOutputPath(job, new Path(args[1]));
	            System.exit(job.waitForCompletion(true) ? 0 : 1);
	        }

}
