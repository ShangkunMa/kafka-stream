package kafka.example.stream.util;

import java.security.MessageDigest;

/**
 * Created by MaShangkun on 18-9-5.
 */
public class GeneralHelper {

    public static boolean isTimeInDruidWindow(Long time) {
        return null != time && time >= makeDruidWindowBegin() && time <= makeDruidWindowEnd();
    }

    public static long makeDruidWindowBegin() {
        long now = System.currentTimeMillis() / 1000;
        long begin = now - now % getDruidInterval();
        long second = now % getDruidInterval();
        if (second < Configurations.getInstance().druidWindowPeriod * 60) {
            return begin - Configurations.getInstance().druidWindowPeriod * 60;
        }
        return begin;
    }

    public static long makeDruidWindowEnd() {
        long now = System.currentTimeMillis() / 1000;
        return now - now % getDruidInterval() + getDruidInterval();
    }


    public static int getDruidInterval() {
        return 3600;
    }

    public static String MD5(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(s.getBytes("utf-8"));
            return toHex(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String toHex(byte[] bytes) {

        final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();
        StringBuilder ret = new StringBuilder(bytes.length * 2);
        for (int i = 0; i < bytes.length; i++) {
            ret.append(HEX_DIGITS[(bytes[i] >> 4) & 0x0f]);
            ret.append(HEX_DIGITS[bytes[i] & 0x0f]);
        }
        return ret.toString();
    }
}