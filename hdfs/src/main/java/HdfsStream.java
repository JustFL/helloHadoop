import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用流操作hdfs的上传下载 这里的输入输出指的是内存 例如上传操作 输入指的是从本地输入到内存中 输出指的是从内存输出到hdfs
 * @author summerKiss
 *
 */

public class HdfsStream {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata01:8020"), conf, "root");

        //如果有就退出
        Path path = new Path("/HdfsStream.java");
        if (fs.exists(path)) {
            return;
        }

        //本地文件系统输入流 使用了相对路径
        FileInputStream in = new FileInputStream(new File("hdfs/src/main/java/HdfsStream.java"));
        //hdfs文件系统输出流 必须指定文件名
        FSDataOutputStream out = fs.create(new Path("/HdfsStream.java"));
        //上传文件
        IOUtils.copyBytes(in, out, 4096);

        //hdfs文件系统输入流
        FSDataInputStream in1 = fs.open(new Path("/HdfsStream.java"));
        //本地文件系统输出流
        FileOutputStream out1 = new FileOutputStream(new File("D:\\HdfsStream.java"));

        /**
         * 设置在流中读取的起始位置
         * public void seek(long desired);
         * 设置读取长度
         * public static void copyBytes(InputStream in, OutputStream out, long count, boolean close);
         */

        //下载文件
        IOUtils.copyBytes(in1, out1, 4096);

        fs.close();
    }
}
