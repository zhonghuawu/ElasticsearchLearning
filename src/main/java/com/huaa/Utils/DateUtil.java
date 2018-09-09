package com.huaa.Utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/9 11:12
 */

public class DateUtil {

    public static String INDEX_DATE_FORMAT = "yyyy-MM-dd";
    public static String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);

    public static String format(Date timestamp) {
        return timestamp.toInstant().atZone(ZoneId.systemDefault()).format(formatter);
    }

    public static String format(Long timestamp) {
        return format(new Date(timestamp));
    }

    public static Date parse(String timestamp) {
        return Date.from(LocalDateTime.parse(timestamp, formatter).atZone(ZoneId.systemDefault()).toInstant());
    }

    private DateUtil() {}



}
