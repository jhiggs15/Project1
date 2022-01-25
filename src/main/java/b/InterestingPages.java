package b;

import javafx.util.Pair;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

public class InterestingPages {

    public static class PageViewMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable whatPage = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            whatPage.set(Integer.parseInt(columns[2]));
            context.write(whatPage, one);
        }
    }

    public static class PageViewCombiner
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
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

    public static class PageViewReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        private IntWritable page = new IntWritable();
        private IntWritable result = new IntWritable();
        private PriorityQueue<Pair<Integer, Integer>> topEight;
//        private HashMap<Integer, Integer> pageViews = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException
        {
            topEight  = new PriorityQueue<Pair<Integer, Integer>>(new Comparator<Pair<Integer, Integer>>() {
                @Override
                public int compare(Pair<Integer, Integer> s1, Pair<Integer, Integer> s2) {
                    return s1.getValue().compareTo(s2.getValue());
                }
            });
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            topEight.add(new Pair<Integer,Integer>(key.get(), sum));
            if(topEight.size() > 8) topEight.poll();
            result.set(sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
//            Pair<Integer, Integer> kv;
            int count = 8;
            for (Pair<Integer, Integer> kv: topEight){
                page.set(kv.getKey());
                result.set(kv.getValue());
                context.write(page, result);
            }
        }
    }

    /**
     * Use run coniditions in intellij to pass the files needed
     * I use output/b.txt as the output
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "interesting pages");
        job.setJarByClass(InterestingPages.class);
        job.setMapperClass(InterestingPages.PageViewMapper.class);
        job.setCombinerClass(InterestingPages.PageViewCombiner.class);
        job.setReducerClass(InterestingPages.PageViewReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
