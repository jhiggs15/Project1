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

import java.io.*;
import java.util.HashSet;
import java.util.Set;


// interprets problem as returning all users even if they have no page accesses or no favorites
public class FavoritesInterpretation2 {

    public enum FavoritesCounters {USERCOUNT, ACCESSCOUNT, NUMBEROFACCESESREAD};

    public static class CountUsersMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(new Text("userCount"), one);
        }
    }

    public static class CountUsersReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (IntWritable val : values)
                context.getCounter(FavoritesCounters.USERCOUNT).increment(1);
        }
    }

    public static class CountNumberOfAccessesMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(new Text("accessCount"), one);
        }
    }

    public static class CountNumberOfAccessesReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (IntWritable val : values)
                context.getCounter(FavoritesCounters.ACCESSCOUNT).increment(1);
        }
    }

    public static class FavoritesMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable byWho = new IntWritable();
        private IntWritable whatPage = new IntWritable();
        private long accessCount;
        private long userCount;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            this.accessCount  = context.getConfiguration().getLong(FavoritesCounters.ACCESSCOUNT.name(), 0);
            this.userCount  = context.getConfiguration().getLong(FavoritesCounters.USERCOUNT.name(), 0);
        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(","); // splits row of data into an array
            byWho.set(Integer.parseInt(columns[1])); // gets byWho value
            whatPage.set(Integer.parseInt(columns[2])); // gets whatPage value
            context.write(byWho, whatPage);
            context.getCounter(FavoritesCounters.NUMBEROFACCESESREAD).increment(1);

            long numberOfAccessRead = context.getCounter(FavoritesCounters.NUMBEROFACCESESREAD).getValue();
            if(numberOfAccessRead == accessCount)
                for(int i = 1; i <= userCount; i++)
                    context.write(new IntWritable(i), new IntWritable(0));
        }
    }

    public static class FavoritesReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numberOfPages = 0;
            Set<Integer> uniquePages = new HashSet<>();
            for (IntWritable whatPage : values) {
                int whatPageValue = whatPage.get();
                if(whatPageValue != 0) {
                    numberOfPages += 1;
                    uniquePages.add(whatPageValue);
                }

            }
            result.set(numberOfPages + ", " + uniquePages.size());
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "user count");
        job1.setJarByClass(FavoritesInterpretation2.class);
        job1.setMapperClass(FavoritesInterpretation2.CountUsersMapper.class);
        job1.setReducerClass(FavoritesInterpretation2.CountUsersReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "access count");
        job2.setJarByClass(FavoritesInterpretation2.class);
        job2.setMapperClass(FavoritesInterpretation2.CountNumberOfAccessesMapper.class);
        job2.setReducerClass(FavoritesInterpretation2.CountNumberOfAccessesReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);

        long userCount = job1.getCounters().findCounter(FavoritesCounters.USERCOUNT).getValue();
        long accessCount = job2.getCounters().findCounter(FavoritesCounters.ACCESSCOUNT).getValue();

        Configuration conf3 = new Configuration();
        conf3.setLong(FavoritesCounters.USERCOUNT.name(), userCount);
        conf3.setLong(FavoritesCounters.ACCESSCOUNT.name(), accessCount);
        Job job3 = Job.getInstance(conf3, "favorites");
        job3.setJarByClass(FavoritesInterpretation2.class);
        job3.setMapperClass(FavoritesInterpretation2.FavoritesMapper.class);
        job3.setReducerClass(FavoritesInterpretation2.FavoritesReducer.class);

        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}

