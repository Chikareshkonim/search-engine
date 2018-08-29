package in.nimbo.moama.util;

import in.nimbo.moama.configmanager.PropertyType;

public enum ElasticPropertyType implements PropertyType {
    ELASTIC_PAGES_TABLE("elastic.pages.table"), ELASTIC_TEST_TABLE("elastic.test.table"),
    ELASTIC_FLUSH_SIZE_LIMIT("elastic.flush.size.limit"), ELASTIC_FLUSH_NUMBER_LIMIT("elastic.flush.number.limit"),
    TEXT_COLUMN("elastic.text.column"), LINK_COLUMN("elastic.link.column"), SERVER_1("elastic.server1"), SERVER_2("elastic.server2"),
    SERVER_3("elastic.server3"), CLIENT_PORT("elastic.client.port"), VECTOR_PORT("elastic.vector.port"),
    CLUSTER_NAME("elastic.cluster.name");

    private String type;

    ElasticPropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
