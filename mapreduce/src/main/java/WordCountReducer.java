import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer的输入就是mapper的输出
 * KEYIN 是mapper输出的单词 类型Text
 * VALUEIN 是mapper输出的标签1 类型IntWritable
 * KEYOUT 是最终经过统计的单词 类型Text
 * VALUEOUT 是最终经过统计的单词出现的次数 类型IntWritable
 *
 * */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text arg0, Iterable<IntWritable> arg1,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
        /**
         * 在从mapper到reducer的过程中 框架对数据进行了分组处理 按照mapper端输出的key的值进行了分组
         * key值相同的为一组 有多少个不同的key就有多少组
         * Text arg0 :每组中那个相同的key
         * Iterable<IntWritable> arg1:每一组中相同的key所对应的全部的value值
         * Context arg2 :用于写出 直接写到hdfs
         *
         * 这个方法的调用频率为一组调用一次
         *
         * */
        int sum = 0;
        for (IntWritable i : arg1) {
            sum += i.get();
        }
        arg2.write(arg0, new IntWritable(sum));
    }
}
