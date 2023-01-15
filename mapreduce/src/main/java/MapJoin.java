import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

/**
 * @author zy
 * map端的join操作 思路是一张大表和一张小表 将小表加载到内存中
 * 在map函数中只对大表进行读取
 *
 */

public class MapJoin {
    static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        // 创建一个容器对小表的数据进行存储
        HashMap<String, String> product = new HashMap<String, String>();

        Text k = new Text();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {

            // 获取内存中存放的小表的路径
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0]);
            System.out.println("path:-----------"+path);
            FSDataInputStream fis = fs.open(path);

            // 将小表的数据存储到容器中
            BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String str = null;
            while ((str = br.readLine()) != null) {
                String[] datas = str.split("\t");
                product.put(datas[0], datas[1]);
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            String pid = datas[1];
            if (product.containsKey(pid)) {
                k.set(value.toString()+"\t"+product.get(pid));
                context.write(k, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:8020");

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoin.class);

        job.setMapperClass(MyMapper.class);

        // 将小表读取到内存中
        job.addCacheFile(new URI("/mapjoin/product.txt"));

        // 如果没有reduce 这个指的就是map端的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 如果没有reduce 必须指定为0个 否则默认有一个
        job.setNumReduceTasks(0);

        // 这里加载只需要加载大表
        FileInputFormat.addInputPath(job, new Path("/mapjoin/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/mapjoin_out1"));

        job.waitForCompletion(true);

        // 只能打jar包运行
    }
}
