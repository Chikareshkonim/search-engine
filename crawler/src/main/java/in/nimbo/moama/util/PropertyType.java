package in.nimbo.moama.util;
public enum PropertyType implements in.nimbo.moama.PropertyType {
    H_BASE_FAMILY_1("hbase.family1"), H_BASE_FAMILY_2("hbase.family2"), H_BASE_COLUMN_OUT_LINKS("hbase.column.outlinks"),
    H_BASE_COLUMN_PAGE_RANK("hbase.column.pagerank"), H_BASE_TABLE("hbase.table"), BOOTSTRAP_SERVER("bootstrap.servers"),
    GROUP_ID("group.id"), AUTO_COMMIT_INTERVAL_MS("auto.commit.interval.ms"), KEY_DESERIALIZER("key.deserializer"),
    VALUE_DESERIALIZER("value.deserializer"), KEY_SERIALIZER("key.serializer"), VALUE_SERIALIZER("value.serializer"),
    MAX_POLL_RECORDS("max.poll.records"), AUTO_OFFSET_RESET("auto.offset.reset"), ENABLE_AUTO_COMMIT("enable.auto.commit");


    public void setType(String type) {


    }

    private String type;
    PropertyType(String type) {
        this.type = type;
    }
    public String toString() {
        return type;
    }
}
