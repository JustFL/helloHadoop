import java.util.Arrays;
/**
 * split() 方法根据匹配给定的正则表达式来拆分字符串
 * 注意： . $ |  * 等转义字符 必须得加 \\
 * 注意：多个分隔符 可以用 | 作为连字符。
 * 语法
 * 	public String[] split(String regex, int limit)
 * 参数
 * 	regex -- 正则表达式分隔符。
 * 	limit -- 分割的份数。
 * @author summerKiss
 *
 */

public class TestSplit {
    public static void main(String[] args) {
        String str = new String("Welcome-to-Runoob");

        System.out.println("- 分隔符返回值 :" );
        for (String retval: str.split("-")){
            System.out.println(retval);
        }

        System.out.println("");
        System.out.println("- 分隔符设置分割份数返回值 :" );
        for (String retval: str.split("-", 2)){
            System.out.println(retval);
        }

        System.out.println("");
        String str2 = new String("www.runoob.com");
        System.out.println("转义字符返回值 :" );
        for (String retval: str2.split("\\.", 3)){
            System.out.println(retval);
        }

        System.out.println("");
        String str3 = new String("acount=? and uu =? or n=?");
        System.out.println("多个分隔符返回值 :" );
        for (String retval: str3.split("and|or")){
            System.out.println(retval);
        }

        //多个分隔符返回值
        //  \\s表示空格 回车 换行等空白符
        System.out.println("");
        String tempAuthorStr="张三;李四,拿破仑，王五；曹操 周瑜";;
        String[] tmpAuthors=tempAuthorStr.split("\\s+|;|,|，|；");
        System.out.println(Arrays.toString(tmpAuthors));


    }
}
