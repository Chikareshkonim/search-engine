package in.nimbo.moama.util;

public enum PropertyType implements in.nimbo.moama.configmanager.PropertyType {
    H_BASE_FAMILY_1("hbase.family1"), H_BASE_FAMILY_2("hbase.family2"), H_BASE_COLUMN_OUT_LINKS("hbase.column.outlinks"),
    H_BASE_COLUMN_PAGE_RANK("hbase.column.pagerank"), H_BASE_TABLE("hbase.table"), BOOTSTRAP_SERVER("bootstrap.servers"),
    GROUP_ID("group.id"), AUTO_COMMIT_INTERVAL_MS("auto.commit.interval.ms"), KEY_DESERIALIZER("key.deserializer"),
    VALUE_DESERIALIZER("value.deserializer"), KEY_SERIALIZER("key.serializer"), VALUE_SERIALIZER("value.serializer"),
    MAX_POLL_RECORDS("max.poll.records"), AUTO_OFFSET_RESET("auto.offset.reset"), ENABLE_AUTO_COMMIT("enable.auto.commit"),
    CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA("crawler.internalLinkAddToKafka"),
    KAFKA_POLL_TIMEOUT_MS("kafka."),
    CRAWLER_POLITE_TIME("Df"),
    CRAWLER_DOMAIN_CHECKER_HASH_PRIME("sd"),
    CRAWLER_NUMBER_OF_THREADS("sd"),
    CRAWLER_START_NEW_THREAD_DELAY_MS("d"),
    CRAWLER_MIN_OF_EACH_THREAD_QUEUE("d"),
    ELASTIC_FLUSH_NUMBER_LIMIT("Ds"),
    ELASTIC_FLUSH_SIZE_LIMIT("ds"),
    ELASTIC_HOSTNAME("ds"),
    ELASTIC_PORT("DS"),
    CRAWLER_THREAD_PRIORITY("SD");

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
