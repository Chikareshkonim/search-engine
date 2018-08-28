package in.nimbo.moama.util;
public enum PropertyType implements in.nimbo.moama.configmanager.PropertyType {
    ELASTIC_PAGES_TABLE("elastic.pages.table"), ELASTIC_TEST_TABLE("elastic.test.table"),
    ELASTIC_FLUSH_SIZE_LIMIT("elastic.flush.size.limit"), ELASTIC_FLUSH_NUMBER_LIMIT("elastic.flush.number.limit"),
    Text_COLUMN("text.column"), LINK_COLUMN("link.column"), SERVER_1("server1"), SERVER_2("server2"), SERVER_3("server3"),
    CLIENT_PORT("client.port"), VECTOR_PORT("vector.port"),CLUSTER_NAME("cluster.name");

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
