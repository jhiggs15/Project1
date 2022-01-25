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
        private static PriorityQueue<Pair<Integer, Integer>> topEight = new PriorityQueue<Pair<Integer, Integer>>(new Comparator<Pair<Integer, Integer>>() {
            @Override
            public int compare(Pair<Integer, Integer> s1, Pair<Integer, Integer> s2) {
                return s1.getValue().compareTo(s2.getValue());
            }
        });
//        private HashMap<Integer, Integer> pageViews = new HashMap<>();

//        @Override
//        public void setup(Context context) throws IOException,
//                InterruptedException
//        {
//            topEight  = new PriorityQueue<Pair<Integer, Integer>>(new Comparator<Pair<Integer, Integer>>() {
//                @Override
//                public int compare(Pair<Integer, Integer> s1, Pair<Integer, Integer> s2) {
//                    return s1.getValue().compareTo(s2.getValue());
//                }
//            });
//        }

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

    public static class PageInfoMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable id = new IntWritable();
        private Text pageInfo = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (value.toString().contains(",")) {
                final String[] columns = value.toString().split(",");
                id.set(Integer.parseInt(columns[0]));
                pageInfo.set(columns[1] +" from "+columns[2]);
                context.write(id, pageInfo);
            } else {
                final String[] columns = value.toString().split("\t");
                id.set(Integer.parseInt(columns[0]));
                pageInfo.set("Interesting");
                context.write(id, pageInfo);
            }
        }
    }

    public static class PageInfoReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        private Text pageInfo = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Boolean isInteresting = false;
            for(Text t : values) {
                if (t.toString().equals("Interesting")){
                    isInteresting = true;
                } else {
                    pageInfo.set(t);
                }
            }
            if(isInteresting){
                context.write(key, pageInfo);
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
//        Configuration conf = new Configuration();
//        Job job1 = Job.getInstance(conf, "interesting pages");
//        job1.setJarByClass(InterestingPages.class);
//        job1.setMapperClass(InterestingPages.PageViewMapper.class);
//        job1.setCombinerClass(InterestingPages.PageViewCombiner.class);
//        job1.setReducerClass(InterestingPages.PageViewReducer.class);
//        job1.setOutputKeyClass(IntWritable.class);
//        job1.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
//        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "interesting pages information");
        job2.setJarByClass(InterestingPages.class);
        job2.setMapperClass(InterestingPages.PageInfoMapper.class);
        job2.setReducerClass(InterestingPages.PageInfoReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job2, new Path(args[2]) +","+ new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}
