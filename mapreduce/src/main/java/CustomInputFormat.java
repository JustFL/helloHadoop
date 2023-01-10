import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * @author zy
 * 自定义输入类 实现将一个文件的内容作为key
 */
public class CustomInputFormat {

    /**
     * 自定义输入类继承FileInputFormat
     * 两个泛型自己定义 这里要读取整个文件 将整个文件的内容读取到key中 value传NullWritable即可
     * */
    static class MyInputFormat extends FileInputFormat<Text, NullWritable>{

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            // 这里将切片和上下文对象通过initialize方法传给文件读取对象 用来获取路径和创建FileSystem对象
            MyRecordReader record = new MyRecordReader();
            record.initialize(split, context);
            return record;
        }
    }

    static class MyRecordReader extends RecordReader<Text, NullWritable>{

        // 文件输入流
        FSDataInputStream open = null;
        // 记录文件长度
        long len= 0;
        // 标记文件知否读取完成
        boolean isFinish = false;
        // key为Text类型 进行存储文件内容
        Text k = new Text();

        /**
         * 这个方法进行初始化 要进行文件读取 要创建一个输入流
         * 创建输入流最重要的是要获取文件的地址 文件地址可以从split切片上获取
         * */
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // 创建FileSystem对象
            FileSystem fs = FileSystem.get(context.getConfiguration());
            // 创建文件路径对象 因为InputSplit是抽象类 所以先强转为其实现类
            FileSplit s = (FileSplit)split;
            // 获取路径
            Path path = s.getPath();
            // 为流对象赋值
            open = fs.open(path);
            // 获取文件长度
            len = split.getLength();
        }

        /**
         * 进行文件读取 返回一个boolean值表示文件是否读取结束
         * */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            if (!isFinish) {
                // 创建一个字节数组来存储文件内容 文件长度根据split获取
                byte[] b = new byte[(int)len];
                // 将文件内容读取到数组中
                open.readFully(0, b);
                // 将字符数组存储到key-value键值对的key中
                k.set(b);
                isFinish = true;
                return isFinish;
            }else {
                isFinish = false;
                return isFinish;
            }
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {

            return k;
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {

            return NullWritable.get();
        }

        /**
         * 文件读取的进度
         * */
        @Override
        public float getProgress() throws IOException, InterruptedException {
            // 这里简单的返回一下 读完返回1.0否则返回0.0
            return isFinish?1.0F:0.0F;
        }

        @Override
        public void close() throws IOException {

            open.close();

        }

    }

    /**
     * 由于自定义了输入类 所以输入的两个参数要按照自定义的类型来
     * */
    static class MyMapper extends Mapper<Text, NullWritable, Text, NullWritable>{
        @Override
        protected void map(Text key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            // 因为是整个文件读取 所以直接发送
            System.out.println("-----"+key.toString()+"-------");
            context.write(key, NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
                              Context context) throws IOException, InterruptedException {
            // map端整个文件发送 中间shuffle过程按照文件内容分组 内容完全一致的分成一组
            context.write(key, NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:8020");

        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomInputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置使用自定义输入类型
        job.setInputFormatClass(MyInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/wordcount"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcountout02"));

        job.waitForCompletion(true);
    }
}
