import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  内容:
 *  1>自定义类Flowbean
 *  2>自定义分区规则
 *  3>Flowbean类自定义排序规则
 * */
class Flowbean implements WritableComparable<Flowbean>{
    private int upflow;
    private int downflow;
    private int sumflow;

    public int getUpflow() {
        return upflow;
    }
    public void setUpflow(int upflow) {
        this.upflow = upflow;
    }
    public int getDownflow() {
        return downflow;
    }
    public void setDownflow(int downflow) {
        this.downflow = downflow;
    }
    public int getSumflow() {
        return sumflow;
    }
    public void setSumflow(int sumflow) {
        this.sumflow = sumflow;
    }
    public void setSumflow() {
        this.sumflow = upflow + downflow;
    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + sumflow;
    }

    //无参构造器必须存在
    public Flowbean() {
        super();
    }

    public Flowbean(int upflow, int downflow) {
        super();
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = this.upflow + this.downflow;
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upflow = in.readInt();
        this.downflow = in.readInt();
        this.sumflow = in.readInt();
    }

    //序列化方法 注意序列化和反序列化的属性顺序必须一致
    @Override
    public void write(DataOutput out) throws IOException {
        //这里特别注意不要用write方法！！！
        out.writeInt(upflow);
        out.writeInt(downflow);
        out.writeInt(sumflow);
    }

    @Override
    public int compareTo(Flowbean o) {
        //先按照总流量倒序排序 再按照上行流量顺序排序
        if (o.sumflow != this.sumflow) {
            return o.sumflow - this.sumflow;
        }else {
            return this.upflow - o.upflow;
        }
    }
}

class DefineMapper extends Mapper<LongWritable, Text, Text, Flowbean>{

    private Text outK = new Text();
    private Flowbean outV = new Flowbean();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException ,InterruptedException {
        String[] split = value.toString().split("\t");

        outK.set(split[1]);
        outV.setUpflow(Integer.parseInt(split[split.length-3]));
        outV.setDownflow(Integer.parseInt(split[split.length-2]));
        outV.setSumflow();
        //验证手机号长度
        if (split[1].length() == 11) {
            context.write(outK, outV);
        }
    }
}

class DefineReducer extends Reducer<Text, Flowbean, Text, Flowbean>{

    private Flowbean outV = new Flowbean();
    protected void reduce(Text key, java.lang.Iterable<Flowbean> values, Context context)
            throws IOException ,InterruptedException {
        int sum_upflow = 0;
        int sum_downflow = 0;
        for (Flowbean flowbean : values) {
            sum_upflow += flowbean.getUpflow();
            sum_downflow += flowbean.getDownflow();
        }

        outV.setUpflow(sum_upflow);
        outV.setDownflow(sum_downflow);
        outV.setSumflow();
        context.write(key, outV);
    }
}

public class SelfDefineClass {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SelfDefineClass.class);

        job.setMapperClass(DefineMapper.class);
        job.setReducerClass(DefineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flowbean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        FileInputFormat.addInputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\flowinput"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Code-Workspace\\helloHadoop\\mapreduce\\flowoutput"));

        job.waitForCompletion(true);
    }
}
