package com.utils;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by ly-lichh on 2018/10/31.
 */
public class DateUtils {

    public static String YYYY_MM_DD = "yyyyMMdd";
    public static String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat sdf = new SimpleDateFormat(YYYY_MM_DD);
    private static SimpleDateFormat sdf1 = new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS);

    /**
     * 获取上一天的时间戳
     * @return
     */
    public static long getLastTimeStamp(){
        try{
            Calendar calendar = Calendar.getInstance();
            // 获取年
            int year = calendar.get(Calendar.YEAR);
            // 获取月，这里需要需要月份的范围为0~11，因此获取月份的时候需要+1才是当前月份值
            int month = calendar.get(Calendar.MONTH) + 1;
            // 获取上一日
            int day = calendar.get(Calendar.DAY_OF_MONTH) - 1;
            String lastDateString = year+"-"+month+"-"+day;
            Date date = sdf.parse(lastDateString);
            return date.getTime();
        }catch (Exception e){
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * 获取当天的时间戳
     * @return
     */
    public static long getDateTimeStamp(){
        try{
            Calendar calendar = Calendar.getInstance();
            // 获取年
            int year = calendar.get(Calendar.YEAR);
            // 获取月，这里需要需要月份的范围为0~11，因此获取月份的时候需要+1才是当前月份值
            int month = calendar.get(Calendar.MONTH) + 1;
            // 获取上一日
            int day = calendar.get(Calendar.DAY_OF_MONTH);
            String lastDateString = year+"-"+month+"-"+day;
            Date date = sdf.parse(lastDateString);
            return date.getTime();
        }catch (Exception e){
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * 将日期转为timestamp
     * @param dateString
     * @return
     * @throws Exception
     */
    public static long formatDateToTimeStamp(String dateString)throws Exception{
        return sdf.parse(dateString).getTime();
    }

    /**
     * 将日期转为下一天的timestamp
     * @param dateString
     * @return
     * @throws Exception
     */
    public static long formatNextDateToTimeStamp(String dateString)throws Exception{
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sdf.parse(dateString));
        calendar.add(Calendar.DAY_OF_MONTH,1);
        return calendar.getTime().getTime();
    }

    /**
     * 将时间戳转化为日期
     * @return
     */
    public static String formatTimeStampToDateString(long timeStamp){
        return sdf.format(new Date(timeStamp));
    }

    /**
     * 将时间戳转化为日期
     * @return
     */
    public static String formatTimeStampToDateTimeString(long timeStamp){
        return sdf1.format(new Date(timeStamp));
    }

    public static void main(String args[])throws Exception{
        String date = DateUtils.formatTimeStampToDateString(1540305174000L);
        System.out.println(date);
        String s =  DateUtils.formatTimeStampToDateTimeString(System.currentTimeMillis());
        System.out.println(s);
        long a = DateUtils.formatDateToTimeStamp("20181015");
        long b = DateUtils.formatDateToTimeStamp("20181016");

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sdf.parse("20181015"));
        calendar.add(Calendar.DAY_OF_MONTH,1);
        long c = calendar.getTime().getTime();
        System.out.println(a);
        System.out.println(b);
        System.out.println(c);

    }


}
