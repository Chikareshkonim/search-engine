package in.nimbo.moama.console;

import asg.cliche.Command;
import in.nimbo.moama.elasticsearch.ElasticManager;
import in.nimbo.moama.Tuple;
import in.nimbo.moama.WebDocumentHBaseManager;
import in.nimbo.moama.elasticsearch.SortResults;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.System.out;

public class Console {
    private ElasticManager elasticManager;
    private WebDocumentHBaseManager webDocumentHBaseManager;

    public Console(){
        elasticManager = new ElasticManager();
        webDocumentHBaseManager = new WebDocumentHBaseManager("pages", "outLinks", "score");
    }

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
        Map<Tuple<String, Date>,Float> results = elasticManager.searchNews(words, false, null);
        showNews(results, false);
    }

    @Command(description = "News Search Optimized with news date")
    public void newsSearchOptimizedWithDate(){
        ArrayList<String> words = new ArrayList<>();
        getInput(words, "");
        Map<Tuple<String, Date>,Float> results = elasticManager.searchNews(words, false, null);
        showNews(results, true);
    }

    @Command(description = "Get Trend Words for A Date")
    public void getTrendWords(){
        out.println("Please enter a valid date in the following format: \"dd/MM/yyyy\"\tExample: \"14/05/1998\"");
        String date = new Scanner(System.in).nextLine();
        Collection<Map<String, Double>> trendWords = new ArrayList<>();
        try {
            trendWords = elasticManager.newsWordTrends(new SimpleDateFormat("EEE, dd MMM yyyy").
                    format(new SimpleDateFormat("dd/MM/yyyy").parse(date)));
        } catch (IOException e) {
            out.println("Elastic currently unavailable!");
            return;
        } catch (ParseException e) {
            out.println("Invalid date format:");
            getTrendWords();
        }
        if (trendWords.size() > 0) {
            for(Map<String, Double> trendWord: trendWords){
                if(trendWord.keySet().size() > 0) {
                    out.println(trendWord.keySet().toArray()[0]);
                }
            }
        }
        else{
            out.println("Sorry! No trends found!");
        }
    }

    @Command(description = "Get Trend News")
    public void getTrendNews(){
        out.println("Please enter a valid date in the following format: \"dd/MM/yyyy\"\tExample: \"14/05/1998\"");
        String date = new Scanner(System.in).nextLine();
        Collection<Map<String, Double>> trendWords = new ArrayList<>();
        try {
            trendWords = elasticManager.newsWordTrends(new SimpleDateFormat("EEE, dd MMM yyyy").
                    format(new SimpleDateFormat("dd/MM/yyyy").parse(date)));
        } catch (IOException e) {
            out.println("Elastic currently unavailable!");
            return;
        } catch (ParseException e) {
            out.println("Invalid date format:");
            getTrendNews();
        }
        ArrayList<String> targetWords = new ArrayList<>();
        if (trendWords.size() > 0) {
            for (Map<String, Double> trendWord : trendWords) {
                if(trendWord.keySet().size() > 0) {
                    targetWords.add((String) trendWord.keySet().toArray()[0]);
                }
            }
        }
        Map<Tuple<String, Date>, Float> results = null;
        try {
            results = elasticManager.searchNews(targetWords, true,
                    new SimpleDateFormat("EEE, dd MMM yyyy").
                            format(new SimpleDateFormat("dd/MM/yyyy").parse(date))
                    );
        } catch (ParseException ignored) {
        }
        showNews(results, false);
    }


    private void showResults(Map<String, Float> results, boolean optimize){
        if(!results.isEmpty()) {
            if(optimize) {
                out.println("Primary Results:");
            }
            else{
                out.println("Results:");
            }
            float maxScore = 0;
            int maxReference = 0;
            ArrayList<Integer> references = new ArrayList<>();
            int i = 1;
            for (Map.Entry result : results.entrySet()) {
                if(optimize) {
                    if((float) result.getValue() > maxScore){
                        maxScore = (float) result.getValue();
                    }
                    references.add(webDocumentHBaseManager.getReference((String) result.getKey()));
                    if (references.get(i - 1) > maxReference) {
                        maxReference = references.get(i - 1);
                    }
                }
                    out.println(i + "\t" + result.getKey() + "\t" + "\"score\": " + result.getValue());
                    i++;
            }
            if(optimize) {
                for (Map.Entry result : results.entrySet()) {
                    result.setValue((0.8) * ((Float) result.getValue() / maxScore) + ((0.2) * references.get(i - 1) / maxReference));
                }
                results = SortResults.sortByValues(results);
                out.println("Optimized results with reference counts:");
                i = 1;
                for (Map.Entry result : results.entrySet()) {
                    out.println(i + "\t" + result.getKey() + "\t\"score\": " + result.getValue());
                    i++;
                }
            }
        }
        else{
            out.println("Sorry! No match found");
        }
    }

    private void showNews(Map<Tuple<String, Date>,Float> results, boolean optimize){
        if(!results.isEmpty()) {
            if(optimize) {
                out.println("Primary Results:");
            }
            else{
                out.println("Results:");
            }
            int i = 1;
            float maxScore = 0;
            long maxDate = 0;
            Tuple<String, Date> news;
            for (Map.Entry result : results.entrySet()) {
                news = (Tuple<String, Date>) result.getKey();
                if(optimize) {
                    if (news.getY().getTime() > maxDate) {
                        maxDate = news.getY().getTime();
                    }
                    if ((float) result.getValue() > maxScore) {
                        maxScore = (float) result.getValue();
                    }
                }
                out.println(i + "\t" + news.getX() + "\t\"date\": " + news.getY() +  "\t\"score\": " + result.getValue());
                i++;
            }
            Date currentDate = new Date();
            if(optimize) {
                i = 1;
                for (Map.Entry result : results.entrySet()) {
                    news = (Tuple<String, Date>) result.getKey();
                    i++;
                    result.setValue((0.7) * ((Float) result.getValue() / maxScore) +
                            (0.3) * (1.0 / (currentDate.getTime() - news.getY().getTime()) * (currentDate.getTime() - maxDate)));
                }
                results = SortResults.sortNews(results);
                out.println("Optimized results:");
                i = 1;
                for (Map.Entry result : results.entrySet()) {
                    news = (Tuple<String, Date>) result.getKey();
                    out.println(i + "\t" + news.getX() + "\t\"date\": " + news.getY() +  "\t\"score\": " + result.getValue());
                    i++;
                }
            }
        }
        else{
            out.println("Sorry! No news found");
        }
    }


    private void getInput(ArrayList<String> list, String type){
        out.println("Please add you desired" + type + " words or phrases for search.");
        out.println("Please Finish entering input by typing : -done-");
        String input = new Scanner(System.in).nextLine();
        while(!input.equals("-done-")){
            list.add(input);
            input = new Scanner(System.in).nextLine();
        }
    }
}
