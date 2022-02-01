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
import java.util.HashMap;

// Use same logic but make it work for any data size
public class HappyAndFamousOneJob {

    public static final IntWritable friendsCounter = new IntWritable(-1);
    public static final IntWritable peopleCounter = new IntWritable(-2);
    private final static Text one = new Text("1");


    public static class FriendMapper
            extends Mapper<Object, Text, IntWritable, Text> {

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
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable personID = new IntWritable();
        private Text personName = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            personID.set(Integer.parseInt(columns[0]));
            personName.set(columns[1]);

            context.write(friendsCounter, one);
            context.write(personID, personName);
        }
    }

    public static class FriendReducer
            extends Reducer<IntWritable,Text,Text,IntWritable> {

        private IntWritable result = new IntWritable();
        private Text name = new Text();
        private double friendsCounterValue;
        private double peopleCounterValue;
        private HashMap<Integer, String> friendsMap = new HashMap<>();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values)
                try {
                    sum += Integer.parseInt(val.toString());
                } catch (final NumberFormatException e) {
                    name.set(val);
                }

            if(key.equals(friendsCounter))
                friendsCounterValue = sum;
            else if(key.equals(peopleCounter))
                peopleCounterValue = sum;
            else
                friendsMap.put(key.get(), name + "," + sum);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            double average = friendsCounterValue / peopleCounterValue;
            for (int id: friendsMap.keySet()) {
                int numberOfFriends = Integer.parseInt(friendsMap.get(id).split(",")[1]);
                if (numberOfFriends > average){
                    String usersName = friendsMap.get(id).split(",")[0];
                    name.set(usersName);
                    result.set(numberOfFriends);
                    context.write(name, result);
                }
            }
        }
    }

    /**
     * Use run conditions in intellij to pass the files needed
     * For example my args are:
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/friends.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/myPage.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Project1/output/h.txt
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "friends above average ");
        job1.setJarByClass(HappyAndFamousOneJob.class);
        job1.setReducerClass(HappyAndFamousOneJob.FriendReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, HappyAndFamousOneJob.FriendMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, HappyAndFamousOneJob.PeopleMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime) + " ms");
    }
}
