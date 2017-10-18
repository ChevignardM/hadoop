import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class Tp1{
public static class PivotMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        long colonne = 0;
        long ligne = key.get();
        /*réglage du splitter en semi-column*/
        for (String num : text.toString().split(";")) {
        	/*pour chaque ligne récupérée on créé alors une nouvelle colonne*/
            context.write(new LongWritable(colonne), new Text(ligne + ";" + num));
            colonne++;
        }
    }
}

public static class PivotReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> texts, Context context) throws IOException, InterruptedException {
        TreeMap<Long, String> ligne = new TreeMap<Long, String>();
        /*Text (input qui nous vient du mapper), on re sélectionne selon le splitter de sortie du mapper ; */
        for (Text text : texts) {
            String[] parts = text.toString().split(";");
            ligne.put(Long.valueOf(parts[0]), parts[1]);
        }
        /*nouveau texte qui récupère les valeures récupérées et concatène*/
        String text = StringUtils.join(ligne.values(), ' ');
        /*cette nouvelle ligne a donc une clé (nulle car on ne veut pas afficher le numéro de ligne), et le texte formé*/
        context.write(null, new Text(text));
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

