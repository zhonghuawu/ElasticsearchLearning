package com.huaa.test.data;

import com.huaa.Utils.DateUtil;

import java.io.Serializable;
import java.util.Date;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/2 22:14
 */

public class Logging extends DataLog implements Serializable {

    private static final long serialVersionUID = 5238297658537295792L;

    public static String index = "logging";

    private String id;
    private String content;
    private String timestamp;

    public Logging() {
        setTimestampNow();
    }

    public Logging(String id, String content) {
        this();
        this.id = id;
        this.content = content;
    }

    public Logging(String id, String content, Date timestamp) {
        this(id, content);
        this.timestamp = DateUtil.format(timestamp);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Date getTimestamp() {
        return DateUtil.parse(timestamp);
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = DateUtil.format(timestamp);
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = DateUtil.format(timestamp);
    }

    public void setTimestampNow() {
        setTimestamp(System.currentTimeMillis());
    }

    @Override
    public String indexPostfixName() {
        return timestamp.substring(0, DateUtil.INDEX_DATE_FORMAT.length());
    }

    @Override
    public String toString() {
        return "Logging{" +
                "id='" + id + '\'' +
                ", content='" + content + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
