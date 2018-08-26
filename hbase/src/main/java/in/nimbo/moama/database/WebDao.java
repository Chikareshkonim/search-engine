package in.nimbo.moama.database;

import in.nimbo.moama.database.webdocumet.WebDocument;

public interface WebDao {
    boolean createTable();

    void put(WebDocument document);
}
