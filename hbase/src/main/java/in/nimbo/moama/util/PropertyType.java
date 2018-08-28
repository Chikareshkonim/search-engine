package in.nimbo.moama.util;
public enum PropertyType implements in.nimbo.moama.configmanager.PropertyType {
    H_BASE_CONTENT_FAMILY("hbase.content.family"), H_BASE_RANK_FAMILY("hbase.rank.family"), H_BASE_COLUMN_PAGE_RANK("hbase.column.pageRank"),
    H_BASE_TABLE("hbase.table"), PUT_SIZE_LIMIT("hbase.put.size.limit"), HBASE_VERSION_FAMILY("hbase.version.family"),
    HBASE_COLUMN_VERSION("hbase.column.version");

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
