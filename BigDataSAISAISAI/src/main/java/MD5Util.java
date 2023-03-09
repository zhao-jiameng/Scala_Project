import java.security.MessageDigest;

/**
 * @description: MD5加密工具类
 * @author: pinnuli
 * @date: 2018-4-23
 */
public class MD5Util {
    //将字节数组中每一个元素通过byteToHexString转换成对应的hexDigits值
    //然后按顺序拼接成字符串通过StringBuffer的toString方法转换为字符串返回
    private static String byteArrayToHexString(byte b[]) {
        StringBuffer resultSb = new StringBuffer();
        for (int i = 0; i < b.length; i++) {
            resultSb.append(byteToHexString(b[i]));
        }
        return resultSb.toString();
    }


    //字节换算成hexDigits对应的值
    private static String byteToHexString(byte b) {
        int n = b;
        if (n < 0) {
            n += 256;
        }
        int d1 = n / 16;
        int d2 = n % 16;
        return hexDigits[d1] + hexDigits[d2];
    }

    public static String MD5Encode(String origin, String charsetname) {
        String resultString = null;
        try {
            resultString = new String(origin);
            MessageDigest md = MessageDigest.getInstance("MD5");
            if (charsetname == null || "".equals(charsetname)) {
                resultString = byteArrayToHexString(md.digest(resultString
                        .getBytes()));
            } else {
                resultString = byteArrayToHexString(md.digest(resultString
                        .getBytes(charsetname)));
            }
        } catch (Exception exception) {

        }
        return resultString;
    }

    private static final String hexDigits[] = {"0", "1", "2", "3", "4", "5",
            "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};


    public static void main(String[] args) {
//        System.out.println(MD5Util.MD5Encode("123", "UTF-8"));
        System.out.println(MD5Util.MD5Encode("123345","UTF-8"));
    }
}
