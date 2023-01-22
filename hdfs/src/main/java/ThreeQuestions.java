import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ThreeQuestions {


    @Test
    public void q1() throws IOException {

        /**
         * 求一个文件中出现次数最多的那个IP
         * 如果是一个非常大的文件 思路是 将大文件分割成为小文件 对小文件进行统计 最后进行汇总
         * */

        File f = new File("config/ipCount.txt");
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);

        HashMap<String, Integer> record = new HashMap<String, Integer>();

        String str;
        while ((str = br.readLine()) != null) {
            if (record.containsKey(str)) {
                int value = record.get(str);
                record.replace(str, value+1);
            }
            else {
                record.put(str, 1);
            }
        }

        int max = 0;
        String maxip = "";
        for (Map.Entry<String, Integer> entry : record.entrySet()) {
//			System.out.println("<"+entry.getKey()+":"+entry.getValue()+">");
            if (entry.getValue() > max) {
                max = entry.getValue();
                maxip = entry.getKey();
            }
        }

        System.out.println("数量最多的IP是:"+maxip);
        br.close();
    }



    @Test
    public void q2() {
        /**
         *
         * @author summerKiss
         * 快速判断给定的ip地址在文件中是否存在
         * BF是由一个长度为m比特的位数组（bit array）与k个哈希函数（hash function）组成的数据结构。
         * 位数组均初始化为0，所有哈希函数都可以分别把输入数据尽量均匀地散列。
         * 当要插入一个元素时，将其数据分别输入k个哈希函数，产生k个哈希值。以哈希值作为位数组中的下标，将所有k个对应的比特置为1。
         * 当要查询（即判断是否存在）一个元素时，同样将其数据输入哈希函数，然后检查对应的k个比特。如果有任意一个比特为0，表明该元素一定不在集合中。
         * 如果所有比特均为1，表明该集合有（较大的）可能性在集合中。
         * 为什么不是一定在集合中呢？因为一个比特被置为1有可能会受到其他元素的影响，这就是所谓“假阳性”（false positive）。
         * 相对地，“假阴性”（false negative）在BF中是绝不会出现的。
         */

    }





    @Test
    public void q3() throws IOException {

        /**
         * 求两个文件中相同的IP
         *
         * 如果是两个非常大的大文件 还是要采取分而治之的思想 假如两个文件都分为4个小文件 编号1-4
         * 但是直接分开 则需要互相比较每个文件 需要比较16次
         * 需要让对应编号文件内的ip范围相同 可以对每个ip求hashcode() 然后对4取余 保证每个文件内的ip范围都是一定的
         * 这样编号相对应的4个文件进行4次比较就可以取出来相同的ip 最后再进行汇总就可以
         *
         */

        File f1 = new File("config/ipCount.txt");
        FileReader fr1 = new FileReader(f1);
        BufferedReader br1 = new BufferedReader(fr1);

        HashSet<String> set1 = new HashSet<String>();
        String str1;
        while ((str1 = br1.readLine()) != null) {
            set1.add(str1);
        }
        br1.close();



        File f2 = new File("config/sameIp.txt");
        FileReader fr2 = new FileReader(f2);
        BufferedReader br2 = new BufferedReader(fr2);

        HashSet<String> set2 = new HashSet<String>();
        String str2;
        while ((str2 = br2.readLine()) != null) {
            set2.add(str2);
        }
        br2.close();

        HashSet<String> result = new HashSet<String>();
        for (String string : set2) {
            if (set1.contains(string)) {
                result.add(string);
            }
        }

        System.out.println(result);


    }




}
