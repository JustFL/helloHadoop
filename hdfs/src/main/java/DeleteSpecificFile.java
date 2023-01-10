import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 删除某个路径下特定类型的文件，比如class类型文件，比如txt类型文件
 *
 * */
public class DeleteSpecificFile {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata01:8020"), conf, "root");

        Path path = new Path("/");
        Delete(fs,path,"mr1_");
    }

    public static void Delete(FileSystem fs, Path path, String string) throws FileNotFoundException, IOException {
        FileStatus[] listStatus = fs.listStatus(path);
        for (FileStatus f : listStatus) {
            if (f.isFile()) {
                if (f.getPath().getName().contains(string)) {
                    fs.delete(f.getPath(), false);
                }
            }else {
                Delete(fs, f.getPath(), string);
            }
        }
    }
}
