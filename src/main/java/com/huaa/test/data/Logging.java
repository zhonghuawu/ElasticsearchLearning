package com.huaa.test.data;

import java.io.Serializable;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/2 22:14
 */

public class Logging implements Serializable {
    private static final long serialVersionUID = 5238297658537295792L;
    private String id;
    private String content;
    private long timestamp;

    public Logging(String id, String content, long timestamp) {
        this.id = id;
        this.content = content;
        this.timestamp = timestamp;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static String indexName() {
        return "logging";
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
