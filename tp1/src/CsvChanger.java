import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map.Entry;

public class CsvChanger {


		public static class Map extends Mapper<LongWritable, Text, LongWritable, MapWritable> {
			
		}
		
		public static class Reduce extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
			
		}

	        
	        String item = "";
	        String cvsSplitBy = ",";
	        int ligne =1;
	      tabs = null;
	        pivots = null;
	        
	      
	        //lecture csv
	        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
	        	

	            while ((item = br.readLine()) != null) {

	                String[] line = item.split(cvsSplitBy);
	                tabs[ligne] = line;
	                ligne ++;
	            }
	            
	            //pivot
		        
		        for(int i=0; i<tabs.length; i++){
		        	for (int j=0; j<tabs[i].length; j++){
		        		pivots[i][j] = tabs[j][i];
		        	}
		        }

	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        
	        public static void main(String[] args) throws Exception {
	            Configuration conf = new Configuration();

	                Job job = new Job(conf, "matrixtranspose");

	            job.setJarByClass(org.myorg.MatrixTranspose.class);    

	            job.setOutputKeyClass(LongWritable.class);
	            job.setOutputValueClass(MapWritable.class);

	            job.setMapperClass(Map.class);
	            job.setReducerClass(Reduce.class);

	            job.setInputFormatClass(TextInputFormat.class);
	            job.setOutputFormatClass(TextOutputFormat.class);

	            FileInputFormat.addInputPath(job, new Path(args[0]));
	            FileOutputFormat.setOutputPath(job, new Path(args[1]));

	            job.waitForCompletion(true);
	        }

}
