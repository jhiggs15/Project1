package h;

import b.InterestingPages;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

// Use same logic but make it work for any data size
public class HappyAndFamous {

    public static final IntWritable friendsCounter = new IntWritable(-1);
    public static final IntWritable peopleCounter = new IntWritable(-2);
    private final static IntWritable one = new IntWritable(1);


    public static class FriendMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable personID = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            personID.set(Integer.parseInt(columns[1]));
            context.write(personID, one);
            context.write(peopleCounter, one);
        }
    }

    public static class PeopleMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(friendsCounter, one);
        }
    }

    public static class FriendCombiner
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class FriendReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        private IntWritable result = new IntWritable();
        private IntWritable whatPage = new IntWritable();
        private double friendsCounterValue;
        private double peopleCounterValue;
        private HashMap<Integer, Integer> friendsMap = new HashMap<>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();

            if(key.equals(friendsCounter))
                friendsCounterValue = sum;
            else if(key.equals(peopleCounter))
                peopleCounterValue = sum;
            else
                friendsMap.put(key.get(), sum);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            double average = friendsCounterValue / peopleCounterValue;
            for (int id: friendsMap.keySet()) {
                Integer numberOfFriends = friendsMap.get(id);
                if (numberOfFriends > average){
                    whatPage.set(id);
                    result.set(numberOfFriends);
                    context.write(whatPage, result);
                }
            }
        }
    }

    public static class NameMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private Text value = new Text();
        private IntWritable id = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            id.set(Integer.parseInt(columns[0])); // id
            value.set(columns[1]); // name
            context.write(id, value);
        }
    }

    public static class AverageMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private Text value = new Text();
        private IntWritable id = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split("\t");
            id.set(Integer.parseInt(columns[0])); // id
            value.set(columns[1]); // average
            context.write(id, value);
        }
    }

    public static class NameFriendsReducer extends Reducer<IntWritable, Text, Text, Text> {

        private Text name = new Text();
        private Text value = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int numberOfFriends = 0;
            for (Text val : values)
                try {
                    numberOfFriends += Integer.parseInt(val.toString());
                } catch (final NumberFormatException e) {
                    name.set(val);
                }

            if(numberOfFriends != 0) {
                value.set("is famous and happy with " + numberOfFriends + " friends");
                context.write(name, value);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "friends above average ");
        job1.setJarByClass(HappyAndFamous.class);
        job1.setCombinerClass(HappyAndFamous.FriendCombiner.class);
        job1.setReducerClass(HappyAndFamous.FriendReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, HappyAndFamous.FriendMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, HappyAndFamous.PeopleMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "temp"));
        job1.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "pages information");
        job2.setJarByClass(HappyAndFamous.class);
        job2.setReducerClass(HappyAndFamous.NameFriendsReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(args[2] + "temp"), TextInputFormat.class, HappyAndFamous.AverageMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, HappyAndFamous.NameMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime) + " ms");
    }
}
