package in.nimbo.moama.util;

import in.nimbo.moama.configmanager.PropertyType;

public enum KeywordPropertyType implements PropertyType {
    HBASE_FAMILY("hbase.family.keys"),
    HBASE_TABLE("hbase.table.keys"),
    CRAWLED_TOPIC("crawled.topic");

    private String type;

    KeywordPropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
