import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 同时进行自定义分组和排序
 * 求所有科目中平均分最高的学生 结果 course name grade
 * @author summerKiss
 *
 */
class ScoreBean implements WritableComparable<ScoreBean>{

    private String course;
    private double grade;

    public String getCourse() {
        return course;
    }
    public void setCourse(String course) {
        this.course = course;
    }
    public double getGrade() {
        return grade;
    }
    public void setGrade(double grade) {
        this.grade = grade;
    }

    public ScoreBean(String course, double grade) {
        super();
        this.course = course;
        this.grade = grade;
    }

    public ScoreBean() {
        super();
    }

    @Override
    public String toString() {
        return "CustomGroupSort [course=" + course + ", grade=" + grade + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeDouble(grade);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.course = in.readUTF();
        this.grade = in.readDouble();
    }

    @Override
    public int compareTo(ScoreBean o) {
        /**
         * double类型相减返回double 要求返回int 所以需要判断
         * 因为我们的需求是要先进行课程分组 再进行分数排序 而框架默认先排序后分组
         * 所以排序时 先将课程排序 这样一样的课程肯定都在一起了 然后再将分数排序
         *
         * 所以遇到此类问题时 需要将分组字段纳入排序范围 并且排序时 先排分组字段 再按照排序字段排序
         */

        if (o.getCourse().compareTo(this.course) == 0) {
            return o.getGrade() - this.getGrade() > 0 ? 1 : (o.getGrade() - this.getGrade() < 0 ? -1 : 0);
        }else {
            return o.getCourse().compareTo(this.course);
        }
    }
}

class GroupSortMapper extends Mapper<LongWritable, Text, ScoreBean, Text>{

    ScoreBean sb = new ScoreBean();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String course = split[0].trim();
        String name = split[1].trim();
        int sum = 0;
        int count = 0;
        for (int i = 2; i < split.length; i++) {
            count++;
            sum+=Integer.parseInt(split[i]);
        }
        double avg = (double)sum / count;
        sb.setCourse(course);
        sb.setGrade(avg);
        v.set(name);
        context.write(sb, v);
    }
}

class GroupSortReducer extends Reducer<ScoreBean, Text, Text, NullWritable>{
    Text k = new Text();
    @Override
    protected void reduce(ScoreBean key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {

//		System.out.println("=============================");
//		for (Text text : values) {
//			System.out.println(key.getCourse()+"\t"+text.toString()+"\t"+key.getGrade());
//		}


        //这里默认key中已经完成分组和排序 只需要取第一个值即可
        Text name = values.iterator().next();
        k.set(key.getCourse()+"\t"+name+"\t"+key.getGrade());
        context.write(k, NullWritable.get());
    }
}

class MyGroup extends WritableComparator{
    //默认情况下第二个参数为false   不会构建实例对象  所以我们需要手动调用一下  传入true
    public MyGroup() {
        super(ScoreBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        /**
         * 分组操作只比较前一条数据和后一条数据 如果相同划分为一组 不同就会重新划分为一组 所以必须先进行排序
         */
        ScoreBean asb=(ScoreBean)a;
        ScoreBean bsb=(ScoreBean)b;
        //只关心返回0的值
        return asb.getCourse().compareTo(bsb.getCourse());
    }
}

public class CustomGroupSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration conf = new Configuration();
        conf.addResource("config/core-site.xml");
        conf.addResource("config/hdfs-site.xml");

        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomGroupSort.class);

        job.setGroupingComparatorClass(MyGroup.class);

        job.setMapperClass(GroupSortMapper.class);
        job.setReducerClass(GroupSortReducer.class);

        job.setMapOutputKeyClass(ScoreBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/score"));
        FileOutputFormat.setOutputPath(job, new Path("/customgroupsort_03"));

        job.waitForCompletion(true);

    }
}
