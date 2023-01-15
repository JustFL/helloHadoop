import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 驱动类：代码提交类
 * 这里运行程序会打成jar包进行运行
 * */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //重点:设置提交代码的用户 一定要最先指定
        System.setProperty("HADOOP_USER_NAME", "root");
        //获取配置文件
        Configuration conf = new Configuration();
        //创建一个job 用来封装mapper和reducer 每一个计算程序叫做一个job
        Job job = Job.getInstance(conf);

        //设置主驱动类 运行时要打成jar包运行
        job.setJarByClass(WordCountDriver.class);

        //设置mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //这里因为泛型是在编译阶段进行检查 运行时就会被擦除 所以打成jar包后需要再指定一次
        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径 args代表运行程序的时候控制台输入的参数  例如args[0]代表第一个参数
        //设置输入路径 需要统计单词的路径
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\wcinput"));
        //设置输出路径 输出路径不能存在 框架怕将原来的文件覆盖
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\wcpartitionoutput"));

        /**
         * 调整切片大小 切片大小和物理的数据存储块大小128M没有关系  只是在默认的情况下是相等的
         * maptask任务的并行度就是指maptask任务的个数
         * 切片的个数决定了maptask任务的个数 切片的个数是由任务中的文件大小 文件个数和切片大小共同决定的
         * 在FileInputFormat类中的getSplits方法
         * long splitSize = computeSplitSize(blockSize, minSize, maxSize);
         * 中 切片的大小是取三个参数的中间值的大小 blockSize大小就是默认的128M minSize和maxSize大小是配置文件中
         * "mapreduce.input.fileinputformat.split.maxsize"
         * "mapreduce.input.fileinputformat.split.minsize"配置项的值
         * 所以如果有大量的小文件的话 可以将切片变小 将maxsize调小 反之如果想要增大切片大小 可将minSize变大
         * 实际情况中不会直接修改配置文件 一般直接在代码中设置
         * */
        //单位Byte
        //FileInputFormat.setMaxInputSplitSize(job, 330);
        //FileInputFormat.setMinInputSplitSize(job, 20*1024*1024);


        /**
         * 设置reducer的并行度 就是启动多少个reducetask任务 是由job.setNumReduceTasks()方法决定的
         * reducer的并行度在显示上就是最终输出结果的文件个数 将所有的结果文件合并起来就是最终结果
         * 所有的结果文件内容都是不重复的 这个的内部实现是根据mapreduce的默认hash分区方式来实现的
         * 分区的作用就是规划每一个reducetask应该计算的数据范围 所以设计分区的时候一定要足够了解数据 否则可能导致分区的数据不均匀 也就是数据倾斜
         * 分区类Partitioner的默认实现类是HashPartitioner
         * 可以自定义分区规则 需要注意的是自定义分区规则时 返回的分区个数应该小于等于reducetask任务的个数
         * 说白了就是保证每一个分区的数据都有一个reducetask任务来进行计算
         * 如果自定义分区个数大于了声明的reducetask任务的个数 就是导致某个分区的数据没有reducetask任务计算 导致报错
         * */

        //设置调起3个reduretask
        job.setNumReduceTasks(3);
        //使用自定义的分区规则
        job.setPartitionerClass(WordCountPartitioner.class);

        //job提交 boolean类型代表是否打印日志
        job.waitForCompletion(true);

        //运行命令 hadoop jar jar包名称 主类的全路径名称 参数
        //hadoop jar /home/hadoop/wordcount01.jar mapreduce.WordCountDriver /in /out


        /**
         * FileInputFormat 文件加载器抽象类 默认使用的是实现类TextInputFormat
         * RecordReader    文件读取器抽象类 默认使用的是实现类LineRecordReader 会将文件内容转化为key-value键值对的形式
         * 其中key为每一行的起始偏移量
         * value为每一行的内容
         *
         * 从mapper到reducer过程中 有shuffle过程 可以对数据进行分组 排序 分区
         * 分组使用的是WritableComparator
         * 排序使用的是WritableComparable
         * 分区使用Partitioner组件 对mapper输出结果进行分类 默认使用的是hashPartitioner
         * map端进行合并的组件 Combiner
         *
         * shuffle的顺序是先排序 在分组
         *
         * 写出到hdfs时候对应的文件写出器和文件写出流参考写入的FileOutputFormat/RecordWriter
         *
         * */

    }
}
