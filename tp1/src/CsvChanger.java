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
		    //l'input reader utilise par défaut le numéro des lignes comme clé
		      int ligne = Integer.parseInt(s[0].trim());
		      String[] datas = s[1].trim().split("\\s+");
		      //nouvelle map pour l'écriture
		      MapWritable tabmap = new MapWritable();
		      int col = 0;
		      for(String d : datas) {
		        int data = Integer.parseInt(d);
		        tabmap.put(new LongWritable(ligne), new IntWritable(data));
		        context.write(new LongWritable(col), tabmap);
		        col++;
			
		      	}
		    }
		}
		
		public static class PivotReduce extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
			
			public void reduce(LongWritable key, Iterable<MapWritable> maps, Context context)
				      throws IOException, InterruptedException {
				        SortedMap<LongWritable,IntWritable> outTab = new TreeMap<LongWritable,IntWritable>();
				        for (MapWritable map : maps) {
				          for(Entry<Writable, Writable>  entry : map.entrySet()) {
				        	  //pour chaque ligne de la map on inscrit les valeures dans la map (output)
				            outTab.put((LongWritable) entry.getKey(),(IntWritable) entry.getValue());
				          }
				        }

				        StringBuffer buffer = new StringBuffer();
				        for(IntWritable line : outTab.values()) {
				          buffer.append(line.toString());
				          buffer.append(" ");
				        }
				        context.write(key,new Text(buffer.toString()));
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
