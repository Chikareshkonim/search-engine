package in.nimbo.moama;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

import static java.lang.Thread.sleep;

public class Utils {
    public static Document getPage(String link){
        try {
            return Jsoup.connect(link).validateTLSCertificates(true).get();
        } catch (IOException ignored) {
        }
        return new Document("  ");

    }

    public static void delay(int delay) {
        try {
            sleep(delay);
        } catch (InterruptedException ignored) {
        }
    }
}
