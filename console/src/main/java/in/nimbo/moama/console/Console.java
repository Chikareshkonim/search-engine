package in.nimbo.moama.console;

import asg.cliche.Command;
import in.nimbo.moama.elasticsearch.ElasticManager;
import in.nimbo.moama.Tuple;
import in.nimbo.moama.WebDocumentHBaseManager;
import in.nimbo.moama.elasticsearch.SortResults;

import java.util.*;

public class Console {
    private ElasticManager elasticManager = new ElasticManager();
    private WebDocumentHBaseManager webDocumentHBaseManager = new WebDocumentHBaseManager("pages", "outLinks", "score");

    @Command(description = "Advanced Search- by necessary, forbidden and preferred statements")
    public void advancedSearch(){
        ArrayList<String> necessaryWords = new ArrayList<>();
        ArrayList<String> forbiddenWords = new ArrayList<>();
        ArrayList<String> preferredWords = new ArrayList<>();
        getInput(necessaryWords, " necessary");
        getInput(forbiddenWords, " forbidden");
        getInput(preferredWords, " preferred");
        Map<String,Float> results = elasticManager.search(necessaryWords,preferredWords,forbiddenWords);
        showResults(results, false);
    }

    @Command(description = "Simple Search")
    public void simpleSearch(){
        ArrayList<String> words = new ArrayList<>();
        getInput(words, "");
        Map<String,Float> results = elasticManager.search(words, new ArrayList<>(), new ArrayList<>());
        showResults(results, false);
    }

    @Command(description = "Simple Search Optimized with Reference Count")
    public void simpleSearchOptimizedWithReferenceCount(){
        ArrayList<String> words = new ArrayList<>();
        getInput(words, "");
        Map<String,Float> results = elasticManager.search(words, new ArrayList<>(), new ArrayList<>());
        showResults(results, true);
    }

    @Command(description = "Advanced Search Optimized with Reference Count")
    public void advancedSearchOptimizedWithReferenceCount(){
        ArrayList<String> necessaryWords = new ArrayList<>();
        ArrayList<String> forbiddenWords = new ArrayList<>();
        ArrayList<String> preferredWords = new ArrayList<>();
        getInput(necessaryWords, " necessary");
        getInput(forbiddenWords, " forbidden");
        getInput(preferredWords, " preferred");
        Map<String,Float> results = elasticManager.search(necessaryWords,preferredWords,forbiddenWords);
        showResults(results, true);
    }

    @Command(description = "News Search")
    public void newsSearch(){
        ArrayList<String> words = new ArrayList<>();
        getInput(words, "");
        Map<Tuple<String, Date>,Float> results = elasticManager.searchNews(words);
        showNews(results, false);
    }

    private void showResults(Map<String, Float> results, boolean optimize){
        if(!results.isEmpty()) {
            if(optimize) {
                System.out.println("Primary Results:");
            }
            else{
                System.out.println("Results");
            }
            int i = 1;
            for (Map.Entry result : results.entrySet()) {
                System.out.println(i + "\t" + result.getKey() + "\t" + "score: " + result.getValue());
                i++;
            }
            if(optimize) {
                i = 1;
                for (Map.Entry result : results.entrySet()) {
                    System.out.println(i + ":");
                    i++;
                    result.setValue((0.8) * (Float) result.getValue() + (0.2) * webDocumentHBaseManager.getReference((String) result.getKey()));
                }
                results = SortResults.sortByValues(results);
                System.out.println("Optimized results with reference counts:");
                i = 1;
                for (Map.Entry result : results.entrySet()) {
                    System.out.println(i + "\t" + result.getKey() + "\t" + "score: " + result.getValue());
                    i++;
                }
            }
        }
        else{
            System.out.println("Sorry! No match found");
        }
    }

    private void showNews(Map<Tuple<String, Date>,Float> results, boolean optimize){
        if(!results.isEmpty()) {
            if(optimize) {
                System.out.println("Primary Results:");
            }
            else{
                System.out.println("Results");
            }
            int i = 1;
            Tuple<String, Date> news;
            for (Map.Entry result : results.entrySet()) {
                news = (Tuple<String, Date>) result.getKey();
                System.out.println(i + "\t" + news.x + "\tdate: " + news.y +  "\tscore: " + result.getValue());
                i++;
            }
//            if(optimize) {
//                i = 1;
//                for (Map.Entry result : results.entrySet()) {
//                    System.out.println(i + ":");
//                    i++;
//                    result.setValue((0.8) * (Float) result.getValue() + (0.2) * webDocumentHBaseManager.getReference((String) result.getKey()));
//                }
//                results = SortResults.sortByValues(results);
//                System.out.println("Optimized results with reference counts:");
//                i = 1;
//                for (Map.Entry result : results.entrySet()) {
//                    System.out.println(i + "\t" + result.getKey() + "\t" + "score: " + result.getValue());
//                    i++;
//                }
//            }
        }
        else{
            System.out.println("Sorry! No news found");
        }
    }


    private void getInput(ArrayList<String> list, String type){
        System.out.println("Please add you desired" + type + " words or phrases for search.");
        System.out.println("Please Finish entering input by typing : -done-");
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();
        while(!input.equals("-done-")){
            list.add(input);
            input = scanner.nextLine();
        }
    }
}
