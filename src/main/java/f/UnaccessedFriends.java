package f;

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

public class UnaccessedFriends {

    public static final int ACCESS_OFFSET = 1000000;

    public static class FriendAccessMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable id = new IntWritable();
        private IntWritable otherPage = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            id.set(Integer.parseInt(columns[1]));
            try {
                Integer.parseInt(columns[3]);
                otherPage.set(Integer.parseInt(columns[2]) + ACCESS_OFFSET);
                // if continues here, this is access log

            } catch (NumberFormatException e) {
                // if catches here it is a friendship log
                otherPage.set(Integer.parseInt(columns[2]));
            }
            context.write(id, otherPage);
        }
    }

    public static class UnaccessedFriendReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private HashMap<Integer, Boolean> friends = new HashMap<>();
        private IntWritable otherPage = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            friends.clear();
//            int page = 0;
            for(IntWritable value: values){
//                int numValue = value.get();
//                if(numValue < ACCESS_OFFSET) {
//                    friends.put(numValue, false);
//                }
                if(!friends.containsKey(value.get() % ACCESS_OFFSET) || !friends.get(value.get() % ACCESS_OFFSET)) {
                    friends.put(value.get() % ACCESS_OFFSET, value.get() > ACCESS_OFFSET);
                }
            }
            for (int page: friends.keySet()) {
                if(!friends.get(page)){
                    otherPage.set(page);
                    context.write(key, otherPage);
                }
            }
        }
    }

//    public static class IDtoNameMapper extends Mapper<Object, Text, Text, Text>

    // 0 - accessLog
    // 1 - friends log
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "unaccessed friends");
        job.setJarByClass(UnaccessedFriends.class);
        job.setMapperClass(UnaccessedFriends.FriendAccessMapper.class);
//        job.setCombinerClass(UnaccessedFriends.IntSumReducer.class);
        job.setReducerClass(UnaccessedFriends.UnaccessedFriendReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job, new Path(args[0]) +","+ new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime) + " ms");
    }
}
