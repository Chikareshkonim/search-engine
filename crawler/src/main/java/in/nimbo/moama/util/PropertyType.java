package in.nimbo.moama.util;

public enum PropertyType {
    H_BASE_FAMILY_1("hbase.family1"), H_BASE_FAMILY_2("hbase.family2"), H_BASE_COLUMN_OUT_LINKS("hbase.column.outlinks"), H_BASE_COLUMN_PAGE_RANK("hbase.column.pagerank"),
    H_BASE_TABLE("hbase.table");

    private String type;

    PropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
