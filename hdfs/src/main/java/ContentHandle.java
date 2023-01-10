import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * 复习流的操作
 * 列举四种读取方式
 * 1>字节流每次读取一个字节
 * 2>转化为字符流 每次读取一个字符
 * 3>转化为BufferedReader 每次读取一行 返回一个string
 * 4>将所有的内容读取到一个字节数组中
 *
 * 使用write方法 将字符串转化为字符数组 写入文件中
 * @author summerKiss
 *
 */

public class ContentHandle {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata01:8020"), conf, "root");

        FSDataInputStream input = fs.open(new Path("/mr1/mr1"));
        FSDataOutputStream output = fs.create(new Path("/mr1/mr2"));

//		//将所有内容读取到一个字节数组中
//		byte[] b = new byte[input.available()];
//		input.read(b);
//		System.out.println(new String(b));

//		//每次读取一个字节
//		int a = 0;
//		while ((a = input.read()) != -1) {
//			System.out.println((char)a);
//		}

//		//每次读取一个字符
//		InputStreamReader inputReader = new InputStreamReader(input);
//		int a = 0;
//		while((a = inputReader.read()) != -1) {
//			System.out.println((char)a);
//		}

        //每次读取一行
        InputStreamReader inputReader = new InputStreamReader(input);
        BufferedReader bufferedReader = new BufferedReader(inputReader);
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            String newLine = line.replaceAll("\\s+", "\t") + "\n";
            System.out.println(newLine);
            output.write(newLine.getBytes());
        }

        bufferedReader.close();
        output.close();
    }
}