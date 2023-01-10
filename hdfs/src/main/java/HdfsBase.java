import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class HdfsBase {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException, URISyntaxException, IOException {

        System.setProperty("HADOOP_USER_NAME", "root");

        /**
         * Configuration对象用来读取配置文件:
         * 1>core-default.xml
         * 2>hdfs-default.xml
         * 3>mapred-default.xml
         * 4>yarn-default.xml
         * hdfs程序在运行时
         * 第一步>默认读取的是jar包中的配置文件
         * 第二步>读取项目的classpath(src)下的配置文件(这个目录下的配置文件名称只能是hdfs-site.xml或者hdfs-default.xml 其他名称无法识别)
         * 可以强制加载别的名字conf.addResource("XXX");
         * 第三步>读取代码中指定配置文件中属性的值
         * conf.set("dfs.replication", "5");
         * */

        Configuration conf = new Configuration();

        /**
         * 指定操作文件系统的用户有三种方式
         * 1>初始化FileSystem对象时 指定用户
         * FileSystem fs = FileSystem.get(new URI("hdfs://192.168.121.10:9000"), conf, "hadoop");
         * 2>使用系统类System来设置用户 一定要放在代码最前面
         * System.setProperty("HADOOP_USER_NAME", "hadoop");
         * 3>运行时指定run as > run configurations > Arguments > VM arguments > -DHADOOP_USER_NAME=hadoop
         * */

        //创建目录树对象 如果不给定URI 则创建本地文件系统 本地指的是代码运行的地方
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata01:8020"), conf);

        //创建目录 可以级联创建
        if (!fs.exists(new Path("/hdfsbase"))) {
            fs.mkdirs(new Path("/hdfsbase"));

            Path src = new Path(HdfsBase.class.getResource("ipCount.txt").toString());
            System.out.println("---------------" + src);
            Path dst = new Path("/hdfsbase");

            fs.copyFromLocalFile(src, dst);
        }

        /**
         * namenode的VERSION文件内容解析
         * 	 namespaceID=1009554514
             clusterID=CID-5253b8a5-c3f0-4549-a445-3c17e7dde8fa --集群标识
             cTime=0
             storageType=NAME_NODE
             blockpoolID=BP-372562848-192.168.121.10-1561277050411 --块池ID 联邦模式下 不同的namenode管理的块池ID不同
             layoutVersion=-63
         * */

        /**
         * 文件上传完成后 真正的存储目录是在datanode上 具体目录为 /home/hadoop/data/hadoopdata/data下
         * current存储真实的数据
         * in_use.lock是锁文件 标识datanode进程 一个节点只能开启一个datanode进程
         * current目录下有一个以namenode的VERSION文件中的blockpoolID为名字的目录 所有的块信息都在这个目录中
         * 最终的存储目录/home/hadoop/data/hadoopdata/data/current/BP-372562848-192.168.121.10-1561277050411/current/finalized/subdir0/subdir0
         * 每个文件在上传过程中会生成两个文件
         * blk_1073741832_1008.meta 原始文件的元数据信息  用于记录原始文件的长度 创建时间 偏移量等信息
         * blk_1073741832  原始文件 后面的数字是Block ID 全局唯一
         * .crc文件是下载的时候生成的 用于校验文件的完整性 校验的是文件的起始偏移量和结尾偏移量 如果中间的内容不发生改变则校验通过
         * 如果中间内容发生改变 则报Checksum error
         * */

        //获取一个目录下的所有文件的具体信息(包括文件的块的具体信息) 不能获取目录
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), false);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            //返回每个文件的块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(blockLocations.length);
            for (BlockLocation blockLocation : blockLocations) {
                //这里输出的是每个文件的每一个块的信息 包括起始偏移量 结尾偏移量和这个块存储在哪个节点上
                System.out.println(blockLocation);
            }
        }

        System.out.println("~~~~~~~~~~~~~");
        //这里可以列出指定文件夹下的所有文件包括文件夹  但是只能显示一些基本信息
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus);
        }


        fs.close();

    }
}
