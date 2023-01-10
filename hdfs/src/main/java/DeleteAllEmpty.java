import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * 删除HDFS集群中的所有空文件和空目录
 * */
public class DeleteAllEmpty {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata01:8020"), conf, "root");

        //Upload(fs, "D:\\BaiduNetdiskDownload\\muti_job.txt", "/mutijobin");
        DeleteAll(fs, "/");
    }

    public static void DeleteAll(FileSystem fs, String path) throws FileNotFoundException, IOException {
        Path p = new Path(path);
        FileStatus[] filestatus = fs.listStatus(p);
        if (filestatus.length == 0) {
            fs.delete(p, false);
        }else {
            for (FileStatus f : filestatus) {
                if (f.isDirectory()) {
                    DeleteAll(fs, f.getPath().toString());
                }else {
                    if (f.getLen() == 0) {
                        fs.delete(f.getPath(), false);
                    }
                }
            }

            //删除子文件子文件夹后 再次检查是否为空
            FileStatus[] afterdelete = fs.listStatus(p);
            if (afterdelete.length == 0) {
                fs.delete(p, false);
            }
        }

    }


}
