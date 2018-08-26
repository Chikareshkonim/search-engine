package in.nimbo.moama.template.util;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

public class Util {
    public static Document getPage(String link){
        try {
            return Jsoup.connect(link).validateTLSCertificates(true).get();
        } catch (IOException e) {
            e.printStackTrace();

        }
        return new Document("  ");

    }

}
