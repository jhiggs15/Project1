package a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class BritishUsers {


    // selection can be done with just map job
    public static class BritishUsersMapper extends Mapper<Object, Text, Text, Text>{
        private Text nationality = new Text();
        private Text nameAndHobby = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(","); // splits row of data into an array

            nationality.set(columns[2]);
            if(nationality.toString().equals("British   ")) {
                nameAndHobby.set(columns[1] + ", " + columns[4]); // combines name and hobby together into a single text object
                context.write(nationality, nameAndHobby);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Find British Users");
        job.setJarByClass(BritishUsers.class);
        job.setMapperClass(BritishUsersMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // myPage.csv
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // a
        job.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println("With Combiner " + seconds + " seconds");
    }
}
