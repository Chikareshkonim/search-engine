package in.nimbo.moama.util;

public enum PropertyType {
    HBASE_FAMILY1("hbase.family1"), HBASE_FAMILY2("hbase.family2"), HBASE_COLUMN_OUTLINKS("hbase.column.outlinks"), HBASE_COLUMN_PAGERANK("hbase.column.pagerank"),
    HBASE_TABLE("hbase.table"), HBASE_TABLE2("hbase.tableTest");

    private String type;

    PropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
