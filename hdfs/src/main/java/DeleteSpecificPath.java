import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 工具类 删除实验生成的目录
 * @author summerKiss
 *
 */
public class DeleteSpecificPath {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.addResource("config/core-site.xml");
        conf.addResource("config/hdfs-site.xml");
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fStatus : listStatus) {
            if (fStatus.isDirectory() && fStatus.getPath().getName().contains("mr1_0")) {
                fs.delete(fStatus.getPath(), true);
            }
        }
    }
}
