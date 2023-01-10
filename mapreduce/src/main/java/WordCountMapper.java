import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * KEYIN:传入的文件的每行的起始偏移量 Long类型 mapreduce底层也是依靠流来操作 使用的是字节流
 * VALUEIN:传入的每行的内容 String类型
 * KEYOUT:单词 String类型
 * VALUEOUT:标签 1 int类型
 * 并且由于在进行数据处理的时候必须要经过网络传输和磁盘持久化 所以数据必须要进行序列化
 * 这里使用hadoop自带的序列化接口writable
 *
 * */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        /**
         * map方法的三个参数
         * LongWritable key每行的偏移量
         * Text value每行的内容
         * Context context上下文对象 用于传输
         * 这个方法的调用频率是每行调用一次
         * 这里的处理思路是 每行调用一次的时候 将每行的内容进行切分 并且对每个单词进行打标签1
         * */
        //每行的内容 value是Text类型 需要反序列化
        String line = value.toString().trim();
        //将每个单词切分成数组 注意split方法传入的是正则表达式 并不是单纯的字符串 所以如果有转义字符 要使用\\
        //具体参见TestSplit
        String[] words = line.split("\\s+|;|,|\\.|!|’");

        Pattern p = Pattern.compile("\\w+");
        //遍历数组 直接发送到reduce端
        for (String word : words) {
            Matcher m = p.matcher(word);
            boolean matches = m.matches();
            if (matches) {
                System.out.println(word);
                Text out_word = new Text(word);
                IntWritable out_tag = new IntWritable(1);
                context.write(out_word, out_tag);
            }

        }
    }
}
