package in.nimbo.moama.util;

import in.nimbo.moama.configmanager.PropertyType;

public enum HBasePropertyType implements PropertyType {
    HBASE_DUPCHECK_COLUMN("hbase.dupcheck.column"), PUT_SIZE_LIMIT("hbase.put.size.limit");

    private String type;

    HBasePropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
