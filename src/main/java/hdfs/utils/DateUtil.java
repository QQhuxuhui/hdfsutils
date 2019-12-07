package hdfs.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Auther: huxuhui
 * @Date: 2018/5/14 17:14
 * @Description: 日期处理类
 */
public class DateUtil {

    public final static String yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";
    /**
     * 日期转字符串
     *
     * @param date   要转的日期
     * @param format 格式
     * @return
     */
    public static String dateToString(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            return sdf.format(date);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 字符串转日期
     *
     * @param date   日期字符串
     * @param format 格式
     * @return 说明：odps的datetime类型兼容datetime和timestamp类型，datetime范围1000-9999年，timestamp范围1970-01-01 08:00:01 到2038-01-19 11:14:07
     * 所以odps的datetime范围1970-01-01 08:00:01 到2038-01-19 11:14:07，超出范围或报错
     */
    public static Date stringToDate(String date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            if (sdf.parse(date).getTime() < 0) {//2038-01-19 11:14:06
                return null;
            } else {
                return sdf.parse(date);
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static Date GetUTCDate() {
        final Calendar cal = Calendar.getInstance();
        //2、取得时间偏移量：
        final int zoneOffset = cal.get(Calendar.ZONE_OFFSET);
        //3、取得夏令时差：
        final int dstOffset = cal.get(Calendar.DST_OFFSET);
        //4、从本地时间里扣除这些差量，即可以取得UTC时间：
        cal.add(Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        return cal.getTime();
    }
}
