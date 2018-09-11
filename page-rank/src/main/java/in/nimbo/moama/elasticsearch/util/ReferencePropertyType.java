package in.nimbo.moama.elasticsearch.util;

import in.nimbo.moama.configmanager.PropertyType;

public enum ReferencePropertyType implements PropertyType {
    H_BASE_CONTENT_FAMILY("hbase.family.outlinks"), H_BASE_TABLE("hbase.table"), HBASE_FAMILY_SCORE("hbase.family.score"),
    HBASE_REFERENCE_COLUMN("hbase.reference.column"), JAR_FILE_ADDRESS("reference.jarfile"), APP_NAME("ranking.appname"), MASTER_ADDRESS("ranking.masteraddress");
    ;

    public void setType(String type) {


    }

    private String type;

    ReferencePropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
