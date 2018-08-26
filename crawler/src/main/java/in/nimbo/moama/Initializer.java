package in.nimbo.moama;

import in.nimbo.moama.crawler.Parser;
import in.nimbo.moama.crawler.language.LangDetector;

public class Initializer {
    public static void initialize() {
        LangDetector langDetector = LangDetector.getInstance();
        Parser.setLangDetector(langDetector);
        langDetector.profileLoad();
    }
}
