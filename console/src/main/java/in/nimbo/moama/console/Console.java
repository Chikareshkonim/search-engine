package in.nimbo.moama.console;



import asg.cliche.Command;
import in.nimbo.moama.database.ElasticDao;
import in.nimbo.moama.database.HBaseDao;
import in.nimbo.moama.util.SortResults;

import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

public class Console {
    ElasticDao elasticDao = new ElasticDao();
    HBaseDao hBaseDao = new HBaseDao();
    String input = "";

    @Command(description = "Advanced Search- by necessary, forbidden and preferred statements")
    public void advancedSearch(){
        ArrayList<String> necessaryWords = new ArrayList<>();
        ArrayList<String> forbiddenWords = new ArrayList<>();
        ArrayList<String> preferredWords = new ArrayList<>();
        getInput(necessaryWords, " necessary");
        getInput(forbiddenWords, " forbidden");
        getInput(preferredWords, " preferred");
        Map<String,Float> results = elasticDao.search(necessaryWords,preferredWords,forbiddenWords);
        showResults(results, false);
    }

    @Command(description = "Simple Search")
    public void simpleSearch(){
        ArrayList<String> words = new ArrayList<>();
        getInput(words, "");
        Map<String,Float> results = elasticDao.search(words, new ArrayList<>(), new ArrayList<>());
        showResults(results, false);
    }

    @Command(description = "Simple Search Optimized with Reference Count")
    public void simpleSearchOptimizedWithReferenceCount(){
        ArrayList<String> words = new ArrayList<>();
        getInput(words, "");
        Map<String,Float> results = elasticDao.search(words, new ArrayList<>(), new ArrayList<>());
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
        Map<String,Float> results = elasticDao.search(necessaryWords,preferredWords,forbiddenWords);
        showResults(results, true);
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
                    result.setValue((0.8) * (Float) result.getValue() + (0.2) * hBaseDao.getReference((String) result.getKey()));
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


    public void getInput(ArrayList<String> list, String type){
        System.out.println("Please add you desired" + type + " words or phrases for search.");
        System.out.println("Please Finish entering input by typing : -done-");
        Scanner scanner = new Scanner(System.in);
        input = scanner.nextLine();
        while(!input.equals("-done-")){
            list.add(input);
            input = scanner.nextLine();
        }
    }

}
