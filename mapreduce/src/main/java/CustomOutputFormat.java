import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author zy
 * 自定义输出类 使平均成绩合格和不合格分别输出到不同的文件夹
 */
public class CustomOutputFormat {

    static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            String str = datas[0] + "\t" + datas[1];
            k.set(str);
            for (int i = 2; i < datas.length; i++) {
                v.set(datas[i]);
                context.write(k, v);
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, DoubleWritable>{

        DoubleWritable d = new DoubleWritable();
        protected void reduce(Text key, java.lang.Iterable<Text> values, Context context)
                throws IOException ,InterruptedException {

            int count = 0;
            int sum = 0;
            for (Text t : values) {
                count++;
                sum+=Integer.parseInt(t.toString());
            }
            double avg = sum / count;
            d.set(avg);
            context.write(key, d);
        };
    }

    static class MyOutputFormat extends FileOutputFormat<Text, DoubleWritable>{

        @Override
        public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            return new MyRecordWriter(fs);
        }

    }

    static class MyRecordWriter extends RecordWriter<Text, DoubleWritable>{

        //创建两个输出流 一个存放及格的 一个存放不及格的
        public FSDataOutputStream pass;
        public FSDataOutputStream fail;
        public FileSystem fs;

        public MyRecordWriter(FileSystem fs) throws IllegalArgumentException, IOException {
            this.fs = fs;
            pass = fs.create(new Path("/outputformat/pass"));
            fail = fs.create(new Path("/outputformat/fail"));
        }

        @Override
        public void write(Text key, DoubleWritable value) throws IOException, InterruptedException {
            if (value.get() >= 60) {
                pass.write((key.toString()+"\t"+value+"\n").getBytes());
            }else {
                fail.write((key.toString()+"\t"+value+"\n").getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            pass.close();
            fail.close();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        //设置文件系统
        conf.set("fs.defaultFS", "hdfs://192.168.121.10:9000");

        Job job = Job.getInstance(conf);
        job.setJarByClass(CustomOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(MyOutputFormat.class);

        //conf中设置了文件系统 所以可以直接写文件系统路径
        FileInputFormat.addInputPath(job, new Path("/scorein"));
        //这里设置是指定了结果标志文件的存放路径
        FileOutputFormat.setOutputPath(job, new Path("/outputformat02"));

        job.waitForCompletion(true);
    }
}

