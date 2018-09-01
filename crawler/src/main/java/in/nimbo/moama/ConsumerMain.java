package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.kafka.MoamaProducer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

public class ConsumerMain {
    public static void main(String[] args) throws IOException {
        ConfigManager configManager = ConfigManager.getInstance();
        configManager.load(new FileInputStream("/home/mohammadreza/IdeaProjects/search-engine/crawler/src/main/resources/test.properties"), ConfigManager.FileType.PROPERTIES);
        Initializer.initialize();
        String helper = "helperSeed";
        String seed = "serverSeed";
        String links = "flink";
        String finalSeed = "finalSeed";
        MoamaConsumer moamaConsumer = new MoamaConsumer(finalSeed, "kafka.my.");
        MoamaProducer moamaProducer = new MoamaProducer("linksTest", "kafka.server.");
        LinkedList<String> urlsToAdd = new LinkedList<>();
        DuplicateHandler hashDuplicateChecker = DuplicateHandler.getInstance();

        sh(moamaConsumer, moamaProducer, urlsToAdd, hashDuplicateChecker);
//        showtopic(moamaConsumer);
    }

    private static void showtopic(MoamaConsumer moamaConsumer) {
        while (true) {
            System.out.println(moamaConsumer.getDocuments().size());
        }
    }

    private static void sh(MoamaConsumer moamaConsumer, MoamaProducer moamaProducer, LinkedList<String> urlsToAdd, DuplicateHandler duplicateChecker) {
        while (true) {
            if (urlsToAdd.size() > 111601) {
                Collections.shuffle(urlsToAdd);
                moamaProducer.pushNewURL(urlsToAdd.toArray(new String[0]));
                urlsToAdd.clear();
                moamaProducer.flush();
            }
            moamaConsumer.getDocuments().forEach(e -> {
                if (duplicateChecker.isDuplicate(e)) {
                }else{
                    duplicateChecker.weakConfirm(e);
                    urlsToAdd.add(e);
                }

            });
            System.out.println(urlsToAdd.size());
        }
    }

}
