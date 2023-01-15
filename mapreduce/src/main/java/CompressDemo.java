import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CompressDemo {

    static class CompressMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        private Text outK = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                outK.set(word);
                context.write(outK, new IntWritable(1));
            }
        }
    }

    static class CompressReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable outV = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable t : values) {
                sum += t.get();
            }
            outV.set(sum);
            context.write(key, outV);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:8020");

        Job job = Job.getInstance(conf);
        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        job.setJarByClass(WordCountSort.class);

        job.setMapperClass(CompressMapper.class);
        job.setReducerClass(CompressReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/wordcount"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount_2"));

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
