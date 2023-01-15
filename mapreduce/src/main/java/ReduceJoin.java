import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author zy
 * 将商品表和订单表进行联合查询的join过程  这里在reduce端进行
 * 首先两张表的关联字段是pid 所以要将pid作为key进行发送
 * 两张表的其他字段作为value 为了在reduce端对数据进行区分 所以要在map端对数据进行打标签
 * product:
 * 01	小米
 * 02	华为
 * 03	格力
 * order:
 * 1001	01	1
 * 1002	02	2
 * 1003	03	3
 * 1004	01	4
 * 1005	02	5
 * 1006	03	6
 *
 * 这种方法的弊端
 * 1 并行度不高
 * 2 容易产生数据倾斜 因为当多个reducetask工作时 按照默认的key值取hash后进行分区 这样热门商品的订单肯定多
 * 3 受到接受数据的容器的性能制约
 * */
public class ReduceJoin {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

        String fileName = new String();
        Text k = new Text();
        Text v = new Text();
        /**
         * setup每个maptask调用一次 也就是每一个切片调用一次
         * 这里获取文件的名称 作为数据的标签  为了在reduce端对数据进行区分
         * */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            fileName = inputSplit.getPath().getName();
        }
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            if (fileName.contains("product")) {
                k.set(datas[0]);
                v.set("PR"+datas[1]);
            }else {
                k.set(datas[1]);
                v.set("OR"+datas[0]+"\t"+datas[2]);
            }
            context.write(k, v);
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, NullWritable>{


        String content = null;
        String res = null;
        Text k = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {

            List<String> prlist = new ArrayList<>();
            List<String> orlist = new ArrayList<>();

            for (Text text : values) {
                content = text.toString();
                System.out.println("content:-------------"+content);
                if (content.startsWith("PR")) {
                    // hadoop框架对这里进行了修改 如果直接add会只保留最后一个对象 前面的对象会被覆盖
                    // 这里需要每次都需要new一个新对象 然后将新对象add进去
                    // 这里没有new新对象是因为substring方法会返回一个new的新对象
                    prlist.add(content.substring(2));
                }else {
                    orlist.add(content.substring(2));
                }
            }

            System.out.println(prlist.size());
            System.out.println("////////////////////////");
            System.out.println(orlist.size());

            //加判断防止有的商品没有订单 只在商品表里有记录
            if (prlist.size() > 0 && orlist.size() > 0) {
                //每一个商品都可能对应多个订单 所以join的时候 循环订单去拼接每一个商品
                for (String order : orlist) {
                    res = order + "\t" + prlist.get(0);
                    k.set(key.toString()+"\t"+res);
                    context.write(k, NullWritable.get());
                }
            }


        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:8020");

        Job job = Job.getInstance(conf);

        job.setJarByClass(ReduceJoin.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/mapjoin"));
        FileOutputFormat.setOutputPath(job, new Path("/reducejoin_1"));

        job.waitForCompletion(true);
    }
}
