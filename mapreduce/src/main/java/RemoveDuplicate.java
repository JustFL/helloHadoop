import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 去重 利用mapper端送出的key value会自动按照key值进行分组的特性
 * 可以将原始数据直接作为key值进行去重复
 * @author summerKiss
 *
 */

class DuplicateMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
        context.write(value, NullWritable.get());
    }
}

class DuplicateReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
    protected void reduce(Text key, java.lang.Iterable<NullWritable> values, Context context) throws java.io.IOException ,InterruptedException {
        context.write(key, NullWritable.get());
    }
}

public class RemoveDuplicate {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:8020");
        Job job = Job.getInstance(conf);

        job.setJarByClass(RemoveDuplicate.class);

        job.setMapperClass(DuplicateMapper.class);
        job.setReducerClass(DuplicateReducer.class);

        //当mapper和reducer的输出类型一致的时候 可以将指定mapper数据类型的语句省略掉
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/duplicateremove"));
        FileOutputFormat.setOutputPath(job, new Path("/duplicateout01"));

        job.waitForCompletion(true);
    }
}
