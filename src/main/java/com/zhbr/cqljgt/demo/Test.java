package com.zhbr.cqljgt.demo;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * @ClassName Test
 * @Description TODO
 * @Autor yanni
 * @Date 2020/11/28 17:32
 * @Version 1.0
 **/
public class Test {

    public static void main(String[] args) throws Exception {
        String s = String.valueOf(System.currentTimeMillis());
        String token = createToken("01000", "cde18afb42556eeed09783b437eece17", s);

        System.out.println(s+"--"+token);

    }

    public static String createToken(String appId, String appKey, String timestamp) throws Exception {
        try {
            //用MD5加密生成秘钥串
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Bytes  = md.digest((appId+appKey+timestamp).getBytes("UTF-8"));
            BigInteger bigInt = new BigInteger(1, md5Bytes);//1代表绝对值
            String token = bigInt.toString(16);
            return token;
        } catch (Exception e) {
            throw e;
        }
    }
}
