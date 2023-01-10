import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * key:map输出的key的类型
 * value:map输出的value的类型
 * */
public class WordCountPartitioner extends Partitioner<Text, IntWritable>{


    /**
     * key:map输出的key的类型
     * value:map输出的value的类型
     * numPartitions：分区个数 job.setNumReduceTasks设置的
     * 自定义分区的返回值对应最终结果文件的编号
     * e.g return 0;对应part-r-00000
     * */
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        char c = key.toString().charAt(0);
        if (c >= 'A' && c <= 'G') {
            return 0;
        }else if (c >= 'H' && c <= 'N') {
            return 1;
        }else {
            return 2;
        }
    }

}
