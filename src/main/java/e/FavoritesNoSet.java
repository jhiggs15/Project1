package e;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// interprets problem as only returning users that have favorites (ie. they have visited at least one others page twice)
public class FavoritesNoSet {

    public static class FavoritesMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable byWho = new IntWritable();
        private IntWritable whatPage = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(","); // splits row of data into an array
            byWho.set(Integer.parseInt(columns[1])); // gets byWho value
            whatPage.set(Integer.parseInt(columns[2])); // gets whatPage value
            context.write(byWho, whatPage);
        }
    }

    public static class FavoritesReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numberOfPages = 0;
//            Set<IntWritable> uniquePages = new HashSet<>();
            for (IntWritable whatPage : values) {
                numberOfPages += 1; // counts the number of pages each user has visited
//                uniquePages.add(whatPage); // tracks unique pages the user visited
            }
            result.set(numberOfPages + "");
            context.write(key, result);
//            if(numberOfPages != uniquePages.size()) {
//                result.set(numberOfPages + ", " + uniquePages.size());
//                context.write(key, result);
//            }

        }
    }

    /**
     * Use run conditions in intellij to pass the files needed
     * For example my args are:
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/accessLog.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Project1/output/e.txt
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "favorites");
        job.setJarByClass(FavoritesNoSet.class);
        job.setMapperClass(FavoritesNoSet.FavoritesMapper.class);
        job.setReducerClass(FavoritesNoSet.FavoritesReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); //accessLog
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //e
        job.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println(seconds + " seconds");
    }
}
