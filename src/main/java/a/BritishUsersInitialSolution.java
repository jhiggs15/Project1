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

public class BritishUsersInitialSolution {
    public static class BritishUsersMapper extends Mapper<Object, Text, Text, Text>{
        private Text nationality = new Text();
        private Text nameAndHobby = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(","); // splits row of data into an array

            nationality.set(columns[2]);
            nameAndHobby.set(columns[1] + ", " + columns[4]); // combines name and hobby together into a single text object

            context.write(nationality, nameAndHobby);

        }
    }

    public static class BritishUsersReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) {

            if(key.toString().equals("British   ")) { // padding needed here because strings must be between 10 and 20 characters
                // iterates through all values for a given key if the nationality is british
                values.forEach((value) -> {
                    result.set(value);
                    try {
                        context.write(key, result); // writes out result
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }

                });

            }

        }
    }


    public static void main(String[] args) throws Exception {

        long timeNow = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Find British Users");
        job.setJarByClass(BritishUsersInitialSolution.class);
        job.setMapperClass(BritishUsersMapper.class);
        job.setCombinerClass(BritishUsersReducer.class);
        job.setReducerClass(BritishUsersReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // myPage
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "2")); // a2
        job.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println(seconds + " seconds");
    }
}
