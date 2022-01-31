package h;

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
import java.util.HashMap;
import java.util.Iterator;

// Use same logic but make it work for any data size
public class HappyAndFamous {

    public static final IntWritable friendsCounter = new IntWritable(-1);
    public static final IntWritable peopleCounter = new IntWritable(-2);

    public static class FriendMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable whatPage = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            try{
                Integer.parseInt(columns[1]);
                whatPage.set(Integer.parseInt(columns[2]));
                context.write(whatPage, one);
                context.write(friendsCounter, one);
            } catch (NumberFormatException e) {
                context.write(peopleCounter, one);
            }
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
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class FriendReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        // There are 20 million friends among 200000 people, so the average friendships per person can be calculated
        private static double numFriends = 0;
        private static double numPeople = 0;

        private IntWritable whatPage = new IntWritable();
        private IntWritable result = new IntWritable();

        private HashMap<Integer, Integer> friendsMap = new HashMap<>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(key.get() == friendsCounter.get()){
                numFriends = sum;
            } else if (key.get() == peopleCounter.get()){
                numPeople = sum;
            } else {
                friendsMap.put(key.get(), sum);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int friends = 0;
            double average = numFriends / numPeople;
            for (int id: friendsMap.keySet()) {
                friends = friendsMap.get(id);
                if (friends > average){
                    whatPage.set(id);
                    result.set(friends);
                    context.write(whatPage, result);
                }
            }
        }
    }

    public static class NameMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text value = new Text();
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (value.toString().contains(",")) {
                final String[] columns = value.toString().split(",");
                id.set(columns[0]);
                value.set(columns[1]);
            } else {
                final String[] columns = value.toString().split("\t");
                id.set(columns[0]);
                value.set(columns[1]);
            }
            context.write(id, value);
        }
    }

    public static class NameFriendsReducer extends Reducer<Text, Text, Text, Text> {

        private Text value = new Text();
        private Text name = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> vl = values.iterator();
            String str = "";
            int count = 0;
            int friends = 0;
            while (vl.hasNext()) {
                String temp = vl.next().toString();
                try {
                    friends = Integer.parseInt(temp);
                } catch (NumberFormatException e) {
                    str = temp;
                }
//                str += vl.next().toString();
                count++;
            }
            if(friends > 0) {
                name.set(str);
                value.set("is famous and happy with " + friends + " friends");
                context.write(name, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "friends above average ");
        job1.setJarByClass(HappyAndFamous.class);
        job1.setMapperClass(HappyAndFamous.FriendMapper.class);
        job1.setCombinerClass(HappyAndFamous.FriendCombiner.class);
        job1.setReducerClass(HappyAndFamous.FriendReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job1, new Path(args[0]) +","+ new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "pages information");
        job2.setJarByClass(HappyAndFamous.class);
        job2.setMapperClass(HappyAndFamous.NameMapper.class);
        job2.setReducerClass(NameFriendsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job2, new Path(args[2]) +","+ new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime) + " ms");
    }
}
