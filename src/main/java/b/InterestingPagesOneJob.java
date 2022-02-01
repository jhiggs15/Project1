package b;

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
import java.util.PriorityQueue;

// I could optimize this by storing the myPage info with the id along the way

public class InterestingPagesOneJob {

    public static class PageViewMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private final static Text one = new Text("1");
        private IntWritable whatPage = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            whatPage.set(Integer.parseInt(columns[2]));
            context.write(whatPage, one);
        }
    }

    public static class PageViewReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        public static class PopPage implements Comparable<PopPage> {
            public int pageId;
            public int accesses;
            public String name;

            public PopPage(int pageId, int accesses, String name){
                this.pageId = pageId;
                this.accesses = accesses;
                this.name = name;
            }

            @Override
            public int compareTo(PopPage s2) {
                return Integer.compare(this.accesses, s2.accesses);
            }
        }

        private IntWritable id = new IntWritable();
        private Text result = new Text();
        private static PriorityQueue<PopPage> topEight = new PriorityQueue<PopPage>();

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
            topEight.add(new PopPage(key.get(), sum, name));
            if(topEight.size() > 8) topEight.poll();
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
//            Pair<Integer, Integer> kv;
            for (PopPage popPage: topEight){
                id.set(popPage.pageId);
                result.set(popPage.name+" has " + popPage.accesses+" accesses");
                context.write(id, result);
            }
        }
    }

    public static class PageInfoMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable id = new IntWritable();
        private Text pageInfo = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(",");
            id.set(Integer.parseInt(columns[0]));
            pageInfo.set(columns[1] +" from "+columns[2]);
            context.write(id, pageInfo);
        }
    }

    /**
     * Use run conditions in intellij to pass the files needed
     * For example my args are:
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/accessLog.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Are-you-My-Friend-Analytics/DataOutput/myPage.csv
     * file:///C:/Users/Gus/Documents/Code/CS-4433/Project1/output/b.txt
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "interesting pages");
        job1.setJarByClass(InterestingPagesOneJob.class);
        job1.setNumReduceTasks(1); //This must be one for this reducer to find the true maximum
        job1.setReducerClass(InterestingPagesOneJob.PageViewReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, InterestingPagesOneJob.PageViewMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, InterestingPagesOneJob.PageInfoMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime) + " ms");
    }

}
