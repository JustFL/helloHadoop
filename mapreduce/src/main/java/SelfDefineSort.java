/**
 *
 * SelfDefineClass已经实现了自定义排序
 * 这里使用SelfDefineClass的结果数据作为输入数据进行排序
 * @author summerKiss
 *
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class DefineSortMapper extends Mapper<LongWritable, Text, Flowbean, Text>{

    private Flowbean outK = new Flowbean();
    private Text outV = new Text();

    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
        String[] split = value.toString().split("\t");
        outK.setUpflow(Integer.parseInt(split[1].trim()));
        outK.setDownflow(Integer.parseInt(split[2].trim()));
        outK.setSumflow(Integer.parseInt(split[3].trim()));
        outV.set(split[0]);

        System.out.println(outK + ":" + outV);
        context.write(outK, outV);
    }
}

class DefineSortReducer extends Reducer<Flowbean, Text, Text, Flowbean>{
    protected void reduce(Flowbean key, java.lang.Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {

        /**
         * 这里的key为自定义类型 每一个自定义对象的地址都不同
         * 但是分组不是按照对象的地址 而是按照自定义类的compareTo方法进行分组的 值相同的分成了一组
         * 所以如果compareTo方法中比较的成员变量有相同的值 那么就会被分为一组 这样的话一组内就会有多个对象  所以必须遍历输出
         * 换言之 如果是按照地址分组 因为地址都是不同的 所以每组中只可能有一个对象 就不需要遍历了
         * 之前的代码使用的是基本类型 则调用的是基本类型的compareTo方法
         * 可以类比set中放自定义类型时候的排序
         * */

        System.out.println("=============");
        for (Text phoneNo : values) {
            System.out.println(key);
            context.write(phoneNo, key);
        }
    }
}

public class SelfDefineSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SelfDefineSort.class);

        job.setMapperClass(DefineSortMapper.class);
        job.setReducerClass(DefineSortReducer.class);

        job.setMapOutputKeyClass(Flowbean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        //将自定义类实验的输出结果作为输入
        FileInputFormat.addInputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\flowoutput"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\selfoutput11"));

        job.waitForCompletion(true);
    }
}
