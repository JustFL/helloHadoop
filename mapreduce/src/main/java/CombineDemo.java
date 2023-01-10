import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 测试CombineInputFormat
 */

class CombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] words = line.split("\\s+|;|,|\\.|!|’");

        Pattern p = Pattern.compile("\\w+");
        for (String word : words) {
            Matcher m = p.matcher(word);
            boolean matches = m.matches();
            if (matches) {
                outK.set(word);
                context.write(outK, outV);
            }

        }
    }
}

 class CombineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

     private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text arg0, Iterable<IntWritable> arg1,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : arg1) {
            sum += i.get();
        }
        outV.set(sum);
        arg2.write(arg0, outV);
    }
}

public class CombineDemo {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CombineDemo.class);

        job.setMapperClass(CombineMapper.class);
        job.setReducerClass(CombineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //如果有大量小文件 推荐使用CombineFileInputFormat 可以将小文件进行合并读取到一个切片中
        //如下代码中如果多个文件大小相加仍然小于切片大小4M 将会只产生一个切片
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineFileInputFormat.setMaxInputSplitSize(job, 4*1024*1024);

        FileInputFormat.addInputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\wcinput"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\wcoutput"));


        job.waitForCompletion(true);
    }

}
