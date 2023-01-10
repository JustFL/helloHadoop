import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

class PartitionSortMapper extends Mapper<LongWritable, Text, Flowbean, Text>{

    private Flowbean outK = new Flowbean();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Flowbean, Text>.Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        outK.setUpflow(Integer.parseInt(split[1]));
        outK.setDownflow(Integer.parseInt(split[2]));
        outK.setSumflow(Integer.parseInt(split[3]));
        outV.set(split[0]);

        context.write(outK, outV);
    }
}

class PartitionSortReducer extends Reducer<Flowbean, Text, Text, Flowbean>{

    @Override
    protected void reduce(Flowbean key, Iterable<Text> values, Reducer<Flowbean, Text, Text, Flowbean>.Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, key);
        }
    }
}

class CustomPartition extends Partitioner<Flowbean, Text>{

    @Override
    public int getPartition(Flowbean flowbean, Text text, int numPartitions) {

        /**
         * 这里注意分区号必须从零开始 逐一累加
         */
        String prefix = text.toString().substring(0,3);
        int partition;
        if (prefix.equals("135")){
            partition = 0;
        }else if (prefix.equals("136")){
            partition = 1;
        }else if (prefix.equals("137")){
            partition = 2;
        }else if (prefix.equals("138")){
            partition = 3;
        }else{
            partition = 4;
        }

        return partition;
    }
}

public class CustomPartitionSort {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomPartitionSort.class);

        job.setMapperClass(PartitionSortMapper.class);
        job.setReducerClass(PartitionSortReducer.class);

        job.setMapOutputKeyClass(Flowbean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        job.setPartitionerClass(CustomPartition.class);

        /**
         * 1.ReduceTasks大于getPartition的结果数 会产生空的结果文件
         * 2.1<ReduceTasks<getPartition的结果数 会因为有的分区数据没有ReduceTask计算而报IO异常
         * 3.如果ReduceTasks=1 则不管MapTask输出多少个分区文件 都会交给这一个ReduceTask 最终只会产生一个结果文件
         */
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\flowoutput"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\flowpartitionsortoutput"));

        job.waitForCompletion(true);

    }


}
