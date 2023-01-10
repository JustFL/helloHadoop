import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 自定义组建combiner 默认情况下没有这个组件 它是在map和reduce之间的一个过程
 * 作用是分担reducertask的压力 因为reducertask过程的并行度不高 而maptask过程的并行度可以很高
 * maptask的并行度和切片直接挂钩  在数据量大的情况下 切片可以有很多 所以maptask的并行度可以很高
 *
 * combiner组件的作用就是在map端进行了一次合并 减少了shuffle过程的数据量 提高了程序效率
 * combiner的业务逻辑和reducer的一摸一样 可以直接照抄reduce的代码 甚至可以直接
 * job.setCombinerClass(WordCountReducer.class);将reducer作为combiner组件直接使用
 * 其本质就是在map端进行了一次reduce
 *
 * 注意! combiner可以用在求和 最大值 最小值 但是不能用来求平均值
 * */

/**
 * 由于combiner是map和reduce之间的一个过程 所以前两个泛型是map的输出 后两个泛型是reduce的输入
 * 因为map的输出就是reduce的输入 所以前两个和后两个泛型是完全一致的
 *
 * */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

    /**
     * 这个reduce方法对应的是一个切片 也就是一个maptask 处理一个切片中的数据 不可以对多个maptask的结果进行合并
     * */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

/**
 * 总结来说
 * Combiner是在每一个MapTask节点运行的
 * Reducer接受全局所有MapTask的输出数据
 *
 * Combiner是对每一个MapTask的输出结果进行局部汇总 以减少网络传输量
 */