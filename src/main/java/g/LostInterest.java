package g;

import f.UnaccessedFriends;
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

public class LostInterest {

    public static class LostInterestMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable byWho = new IntWritable();
        private IntWritable accessTime = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(","); // splits row of data into an array
            byWho.set(Integer.parseInt(columns[1])); // gets byWho value
            accessTime.set(Integer.parseInt(columns[4])); // gets accessTime value
            context.write(byWho, accessTime);
        }
    }

    public static class LostInterestReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
        private Text result = new Text();
        private final int numberOfDays = 5;
        private final int lostInteresetAfteX5Days = numberOfDays * 86400; // days * (24h/day * 60min/h * 60sec/min)
        private final int currentTime = 1000000; // days * (24h/day * 60min/h * 60sec/min)

        /**
         * calculates days since the current time
         * @param epochTime the number of seconds that have elapsed since January 1, 1970 (midnight UTC/GMT)
         * @return the number of days since the current time to two decimal places
         */
        private double findDaysSince(int epochTime) {
            int epochTimeSince = currentTime - epochTime;
            double days = (((epochTimeSince) / 60.0) / 60.0) / 24.0;
            return Math.round(days * 100.0)/100.0;
        }

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int maxAccessTime = 0;
            for (IntWritable usersAccessTime : values) {
                int usersAccessTimeValue = usersAccessTime.get();
                if(usersAccessTimeValue > maxAccessTime)
                    maxAccessTime = usersAccessTimeValue;
            }
            if(currentTime - maxAccessTime >= lostInteresetAfteX5Days)
                context.write(key, new Text(findDaysSince(maxAccessTime) + " days since last access." ));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(LostInterest.class);
        job.setMapperClass(LostInterest.LostInterestMapper.class);
        job.setReducerClass(LostInterest.LostInterestReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}