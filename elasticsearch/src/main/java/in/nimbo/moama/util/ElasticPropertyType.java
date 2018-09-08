package in.nimbo.moama.util;

import in.nimbo.moama.configmanager.PropertyType;

public enum ElasticPropertyType implements PropertyType {
    ELASTIC_PAGES_TABLE("elastic.pages.table"),
    ELASTIC_FLUSH_SIZE_LIMIT("elastic.flush.size.limit"),
    TEXT_COLUMN("elastic.text.column"),
    LINK_COLUMN("elastic.link.column"),
    SERVERS("elastic.servers."),
    CLIENT_PORT("elastic.client.port"),
    VECTOR_PORT("elastic.vector.port"),
    CLUSTER_NAME("elastic.cluster.name"),
    ELASTIC_NUMBER_OF_KEYWORDS("elastic.number.of.keywords"),
    BULK_ACTION_NUMBER_LIMIT("elastic.action.number.limit"),
    BULK_SIZE_MB_LIMIT("elastic.bulk.size.limit.mb"),
    BULK_TIME_INTERVALS_MS("elastic.bulk.time.interval.ms"),
    BULK_CONCURRENT_REQUEST_NUMBER("elastic.bulk.concurrent.plusrequest"),
    BULK_RETRIES_PUT("elastic.bulk.retries.put");

    private String type;

    ElasticPropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
