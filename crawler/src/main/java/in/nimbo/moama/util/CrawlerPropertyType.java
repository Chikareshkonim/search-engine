package in.nimbo.moama.util;


import in.nimbo.moama.configmanager.PropertyType;

public enum CrawlerPropertyType implements PropertyType {
    HBASE_FAMILY_1("hbase.family1"), HBASE_FAMILY_2("hbase.family2"), HBASE_COLUMN_OUT_LINKS("hbase.column.outlinks"),
    HBASE_COLUMN_PAGE_RANK("hbase.column.pagerank"), HBASE_TABLE("hbase.table"), BOOTSTRAP_SERVER("bootstrap.servers"),
    GROUP_ID("group.id"), AUTO_COMMIT_INTERVAL_MS("auto.commit.interval.ms"), KEY_DESERIALIZER("key.deserializer"),
    VALUE_DESERIALIZER("value.deserializer"), KEY_SERIALIZER("key.serializer"), VALUE_SERIALIZER("value.serializer"),
    MAX_POLL_RECORDS("max.poll.records"), AUTO_OFFSET_RESET("auto.offset.reset"), ENABLE_AUTO_COMMIT("enable.auto.commit");


    private String type;

    CrawlerPropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
