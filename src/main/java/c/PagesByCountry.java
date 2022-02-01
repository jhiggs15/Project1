package c;

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

public class PagesByCountry {

    // in report say that we used same
    // add combiner here to reduce load
    public static class PagesByCountryMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable countryCode = new IntWritable();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(","); // splits row of data into an array
            countryCode.set(Integer.parseInt(columns[3])); // gets country code
            context.write(countryCode, one);
        }
    }

    public static class PagesByCountryReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Use run conditions in intellij to pass the files needed
     * For example my args are:
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/myPage.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Project1/output/c.txt
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pages by country");
        job.setJarByClass(PagesByCountry.class);
        job.setMapperClass(PagesByCountry.PagesByCountryMapper.class);
        job.setCombinerClass(PagesByCountry.PagesByCountryReducer.class);
        job.setReducerClass(PagesByCountry.PagesByCountryReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //myPage
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //c
        job.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println(seconds + " seconds");

    }
}
