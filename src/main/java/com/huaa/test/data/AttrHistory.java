package com.huaa.test.data;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/1 10:39
 */

public class AttrHistory extends LogData {

    private static String indexName = "attr_history";

    private String iotId;
    private String attrName;
    private String attrValue;
    private String timestamp;

    public AttrHistory(String iotId, String attrName, String attrValue, String timestamp) {
        this.iotId = iotId;
        this.attrName = attrName;
        this.attrValue = attrValue;
        this.timestamp = timestamp;
    }

    public String getIotId() {
        return iotId;
    }

    public void setIotId(String iotId) {
        this.iotId = iotId;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public String getAttrValue() {
        return attrValue;
    }

    public void setAttrValue(String attrValue) {
        this.attrValue = attrValue;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AttrHistory{" +
                "iotId='" + iotId + '\'' +
                ", attrName='" + attrName + '\'' +
                ", attrValue='" + attrValue + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    public static String indexName() {
        return indexName;
    }
}
