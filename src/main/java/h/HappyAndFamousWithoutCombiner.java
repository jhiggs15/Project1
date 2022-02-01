package h;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Use same logic but make it work for any data size
public class HappyAndFamousWithoutCombiner {

    private enum HappyAndFamousCounters {FRIENDSCOUNTER, PEOPLECOUNTER}

    private final static IntWritable one = new IntWritable(1);

    public static class FriendMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.getCounter(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.FRIENDSCOUNTER).increment(1);
        }
    }

    public static class PeopleMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.getCounter(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.PEOPLECOUNTER).increment(1);
        }
    }

    public static class AvgFriendMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable personID = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            personID.set(Integer.parseInt(columns[1]));
            context.write(personID, one);
        }
    }

    public static class AvgFriendReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {

        private double average;
        private Text value = new Text();

        @Override
        protected void setup(Reducer<IntWritable, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            double peopleCounter = context.getConfiguration().getLong(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.PEOPLECOUNTER.name(), 0);
            double friendsCounter = context.getConfiguration().getLong(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.FRIENDSCOUNTER.name(), 0);
            this.average = friendsCounter / peopleCounter;
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numberOfFriends = 0;
            for (IntWritable val : values)
                numberOfFriends += val.get();

            if (numberOfFriends > average) {
                value.set("is famous and happy with " + numberOfFriends + " friends");
                context.write(key, value);
            }
        }

    }


    /**
     * Use run conditions in intellij to pass the files needed
     * For example my args are:
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/friends.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/myPage.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Project1/output/h
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "friends above average ");
        job1.setJarByClass(HappyAndFamousWithoutCombiner.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, HappyAndFamousWithoutCombiner.FriendMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, HappyAndFamousWithoutCombiner.PeopleMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "temp"));
        job1.waitForCompletion(true);

        long peopleCount = job1.getCounters().findCounter(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.PEOPLECOUNTER).getValue();
        long friendCount = job1.getCounters().findCounter(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.FRIENDSCOUNTER).getValue();

        Configuration conf2 = new Configuration();
        conf2.setLong(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.PEOPLECOUNTER.name(), peopleCount);
        conf2.setLong(HappyAndFamousWithoutCombiner.HappyAndFamousCounters.FRIENDSCOUNTER.name(), friendCount);
        Job job2 = Job.getInstance(conf2, "friends above average ");
        job2.setJarByClass(HappyAndFamousWithoutCombiner.class);
        job2.setReducerClass(AvgFriendReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, HappyAndFamousWithoutCombiner.AvgFriendMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime)/1000.0 + " seconds");

    }
}
