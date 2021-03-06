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
public class HappyAndFamous {

    private enum HappyAndFamousCounters {FRIENDSCOUNTER, PEOPLECOUNTER}

    private final static IntWritable one = new IntWritable(1);

    public static class FriendMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.getCounter(HappyAndFamous.HappyAndFamousCounters.FRIENDSCOUNTER).increment(1);
        }
    }

    public static class PeopleMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.getCounter(h.HappyAndFamous.HappyAndFamousCounters.PEOPLECOUNTER).increment(1);
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

    public static class AvgFriendCombiner
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        private IntWritable value = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numberOfFriends = 0;
            for (IntWritable val : values)
                numberOfFriends += val.get();

            value.set(numberOfFriends);
            context.write(key, value);
        }
    }

    public static class AvgFriendReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {

        private double average;
        private Text value = new Text();

        @Override
        protected void setup(Reducer<IntWritable, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            double peopleCounter = context.getConfiguration().getLong(h.HappyAndFamous.HappyAndFamousCounters.PEOPLECOUNTER.name(), 0);
            double friendsCounter = context.getConfiguration().getLong(h.HappyAndFamous.HappyAndFamousCounters.FRIENDSCOUNTER.name(), 0);
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
        job1.setJarByClass(h.HappyAndFamous.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, h.HappyAndFamous.FriendMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, h.HappyAndFamous.PeopleMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "temp"));
        job1.waitForCompletion(true);

        long peopleCount = job1.getCounters().findCounter(HappyAndFamous.HappyAndFamousCounters.PEOPLECOUNTER).getValue();
        long friendCount = job1.getCounters().findCounter(HappyAndFamous.HappyAndFamousCounters.FRIENDSCOUNTER).getValue();

        Configuration conf2 = new Configuration();
        conf2.setLong(h.HappyAndFamous.HappyAndFamousCounters.PEOPLECOUNTER.name(), peopleCount);
        conf2.setLong(h.HappyAndFamous.HappyAndFamousCounters.FRIENDSCOUNTER.name(), friendCount);
        Job job2 = Job.getInstance(conf2, "friends above average ");
        job2.setJarByClass(h.HappyAndFamous.class);
        job2.setCombinerClass(AvgFriendCombiner.class);
        job2.setReducerClass(AvgFriendReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, h.HappyAndFamous.AvgFriendMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime)/1000.0 + " seconds");


    }
}
