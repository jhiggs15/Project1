package d;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class HappinessFactorOneJob {


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
            extends Reducer<IntWritable,Text,Text,Text> {

        private Text value = new Text();
        private Text id = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String name = "";
            for (Text val : values) {
                try {
                    sum += Integer.parseInt(val.toString());
                } catch (NumberFormatException e){
                    name = val.toString();
                }

            }
            id.set(name);
            value.set("has " +sum+" friends");
            context.write(id, value);
        }
    }

    public static class NameMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private Text value = new Text();
        private IntWritable id = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            id.set(Integer.parseInt(columns[0]));
            value.set(columns[1]);
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
            int friends = 0;
            while (vl.hasNext()) {
                String temp = vl.next().toString();
                try {
                    friends = Integer.parseInt(temp);
                } catch (NumberFormatException e) {
                    str = temp;
                }
            }
            name.set(str);
            value.set("has " +friends+" friends");
            context.write(name, value);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "friends above average ");
        job1.setJarByClass(HappinessFactorOneJob.class);
        job1.setReducerClass(HappinessFactorOneJob.FriendReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, HappinessFactorOneJob.FriendMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, HappinessFactorOneJob.NameMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime) + " ms");
    }
}
