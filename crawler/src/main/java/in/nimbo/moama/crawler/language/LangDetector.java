package in.nimbo.moama.crawler.language;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import in.nimbo.moama.exception.IllegalLanguageException;

import java.io.IOException;
import java.util.List;


public class LangDetector {
    private static LangDetector langDetector=new LangDetector();
    private List<LanguageProfile> languageProfiles;
    private LanguageDetector languageDetector;

    private LangDetector() {
    }

    public static LangDetector getInstance() {
        return langDetector;
    }
    public void profileLoad() {
        try {
            languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        } catch (IOException e) {
            e.printStackTrace();
        }
        languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
    }

    public void languageCheck(String text) throws IllegalLanguageException {
        try {
            TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
            TextObject textObject = textObjectFactory.forText(text);
            Optional<LdLocale> lang = languageDetector.detect(textObject);
            if (!lang.get().getLanguage().equals("en"))
                throw new IllegalLanguageException();
        }catch (Exception e){
            throw new IllegalLanguageException();
        }
    }
}