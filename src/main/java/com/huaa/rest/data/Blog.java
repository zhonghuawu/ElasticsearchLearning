package com.huaa.rest.data;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/12/9 0:55
 */

public class Blog implements Serializable {

    private static final long serialVersionUID = -8570814277602330021L;

    private String title;
    private String text;
    private long views;
    private List<String> tags;
    private long timestamp;

    public Blog(String title, String text) {
        this.title = title;
        this.text = text;
        this.views = 0L;
        this.tags = Lists.newArrayList();
        this.timestamp = System.currentTimeMillis();
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getViews() {
        return views;
    }

    public void setViews(long views) {
        this.views = views;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public void addTags(List<String> tags) {
        this.tags.addAll(tags);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Blog{" +
                "title='" + title + '\'' +
                ", text='" + text + '\'' +
                ", views=" + views +
                ", tags=" + tags +
                ", timestamp=" + timestamp +
                '}';
    }
}
