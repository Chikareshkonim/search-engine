package in.nimbo.moama.util;

import in.nimbo.moama.configmanager.PropertyType;

public enum HBasePropertyType implements PropertyType {
    HBASE_OUTLINKS_FAMILY("hbase.family.outlinks"), HBASE_SCORE_FAMILY("hbase.family.score"), HBASE_COLUMN_PAGE_RANK("hbase.column.pageRank"),
    PUT_SIZE_LIMIT("hbase.put.size.limit");

    private String type;

    HBasePropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
