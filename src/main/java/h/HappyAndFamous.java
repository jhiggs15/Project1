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
import java.util.Iterator;

public class HappyAndFamous {

    public static class FriendMapper
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

    public static class FriendReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        // There are 20 million friends among 200000 people, so the average friendships per person can be calculated
        private static final double AVERAGE_FRIENDS = 20000000 / 200000;

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(sum > AVERAGE_FRIENDS) {
                result.set(sum);
                context.write(key, result);
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
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "interesting pages");
        job1.setJarByClass(HappyAndFamous.class);
        job1.setMapperClass(HappyAndFamous.FriendMapper.class);
        job1.setReducerClass(HappyAndFamous.FriendReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "interesting pages information");
        job2.setJarByClass(HappyAndFamous.class);
        job2.setMapperClass(HappyAndFamous.NameMapper.class);
        job2.setReducerClass(NameFriendsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job2, new Path(args[2]) +","+ new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}