import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 将wordcount程序的统计结果按照词频进行倒序排列
 * 在map到reduce过程中 有默认的按照key值进行的升序排序
 * 实现方法就是将统计结果的key和value进行互换
 * 将词频作为key 单词作为value 再次进行一次map到reduce的过程进行排序
 * 最后输出的时候再反转回来
 * */
public class WordCountSort {
    static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //这里的原始数据是经过词频统计的结果 默认分隔符是\t
            String[] datas = value.toString().split("\t");
            String word = datas[0];
            int count = Integer.parseInt(datas[1]);
            //将词频作为key 将单词作为value进行发送 因为默认是升序 所以取负数进行发送
            context.write(new IntWritable(-count), new Text(word));

        }
    }

    static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //由于词频可能重复 所以需要遍历 将词频取相反数发送
            for (Text t : values) {
                context.write(t, new IntWritable(-key.get()));
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //重点:设置提交代码的用户 一定要最先指定
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //获取配置文件
        Configuration conf = new Configuration();
        //创建一个job 用来封装mapper和reducer
        Job job = Job.getInstance(conf);

        //设置主驱动类
        job.setJarByClass(WordCountSort.class);

        //设置mapper和reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\wcoutput"));
        //设置输出路径 输出路径不能存在 框架怕将原来的文件覆盖
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\wcsortoutput"));

        job.waitForCompletion(true);
    }
}
