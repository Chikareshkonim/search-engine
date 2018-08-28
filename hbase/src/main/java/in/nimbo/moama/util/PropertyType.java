package in.nimbo.moama.util;
public enum PropertyType implements in.nimbo.moama.configmanager.PropertyType {
    H_BASE_FAMILY_1("hbase.family1"), H_BASE_FAMILY_2("hbase.family2"), H_BASE_COLUMN_PAGE_RANK("hbase.column.pageRank"),
    H_BASE_TABLE("hbase.table");


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
