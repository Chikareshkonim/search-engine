package in.nimbo.moama.util;
public enum PropertyType implements in.nimbo.moama.configmanager.PropertyType {
    H_BASE_CONTENT_FAMILY("hbase.family.outlinks"), H_BASE_TABLE("hbase.table"), HBASE_FAMILY_SCORE("hbase.family.score"),
    HBASE_REFERENCE_COLUMN("hbase.reference.column");

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
