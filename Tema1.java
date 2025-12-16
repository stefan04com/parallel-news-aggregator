import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class Tema1 {

    private static int NUM_THREADS = 1;
    private static String articlesPath;
    private static String inputsPath;

    private static final Queue<String> fileQueue = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger processedArticleIndex = new AtomicInteger(0);

    private static String[] languages;
    private static String[] categories;
    private static Set<String> englishLingingWords = ConcurrentHashMap.newKeySet();

    static Map<String, List<String>> categoryToUuids = new ConcurrentHashMap<>();
    static Map<String, List<String>> languageToUuids = new ConcurrentHashMap<>();
    static Map<String, Integer> keywordsCount = new ConcurrentHashMap<>();
    static Map<String, Integer> authorCounts = new ConcurrentHashMap<>();
    static Map<String, LongAdder> categoryCounts = new ConcurrentHashMap<>();

    private static int uniqueCount = 0;
    private static int duplicatesCount = 0;
    private static String bestAuthor = "";
    private static int bestAuthorCount = 0;
    private static String topLanguage = "";
    private static int topLanguageCount = 0;
    private static String topCategory = "";
    private static int topCategoryCount = 0;
    private static String mostRecentArticle = "";
    private static String topKeyword = "";
    private static int topKeywordCount = 0;

    private static List<Article> articles = new ArrayList<>();
    private static final Pattern TEXT_PATTERN = Pattern.compile("[^a-z\\s]");

    static class ProcessingResult {
        Map<String, List<String>> localCatToUuids = new HashMap<>();
        Map<String, List<String>> localLangToUuids = new HashMap<>();
        Map<String, Integer> localKeywords = new HashMap<>();
        Map<String, Integer> localAuthors = new HashMap<>();
        Map<String, Integer> localCatCounts = new HashMap<>();
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Invalid number of arguments");
            System.exit(1);
        }

        init(args);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        String[] pathToArticles = DataLoader.readPaths(articlesPath);
        String[] pathToInputs = DataLoader.readPaths(inputsPath);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        // Load Articles
        fileQueue.addAll(Arrays.asList(pathToArticles));
        List<Future<List<Article>>> readFutures = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            readFutures.add(executor.submit(() -> DataLoader.readFilesDynamic(objectMapper, fileQueue)));
        }

        for (Future<List<Article>> f : readFutures) {
            try {
                articles.addAll(f.get());
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Read Error: " + e.getMessage());
            }
        }
        readFutures.clear();

        // Load Inputs
        List<Future<?>> futures = new ArrayList<>();
        for (String path : pathToInputs) {
            Path inputFile = Path.of(path);
            List<String> lines;
            try {
                lines = Files.readAllLines(inputFile);
            } catch (IOException e) {
                System.err.println("Read error: " + path);
                continue;
            }

            int N = Integer.parseInt(lines.get(0).trim());
            String lower = inputFile.getFileName().toString().toLowerCase();

            if (lower.contains("languages")) {
                languages = new String[N];
            } else if (lower.contains("categories")) {
                categories = new String[N];
            }

            for (int i = 0; i < NUM_THREADS; i++) {
                final int id = i;
                futures.add(executor.submit(() ->
                        DataLoader.readChunkInputs(path, id, NUM_THREADS, lines, N, lower, languages, categories, englishLingingWords)
                ));
            }
        }
        waitForFutures(futures);
        futures.clear();

        // Deduplicate Articles
        duplicatesCount = articles.size();
        int estimatedSize = articles.size();
        Map<String, Integer> uuidCounts = new ConcurrentHashMap<>(estimatedSize);
        Map<String, Integer> titleCounts = new ConcurrentHashMap<>(estimatedSize);

        for (int i = 0; i < NUM_THREADS; i++) {
            final int id = i;
            futures.add(executor.submit(() -> {
                int total = articles.size();
                int chunkSize = (int) Math.ceil((double) total / NUM_THREADS);
                int start = id * chunkSize;
                int end = Math.min(start + chunkSize, total);

                for (int j = start; j < end; j++) {
                    Article a = articles.get(j);
                    uuidCounts.merge(a.getUuid(), 1, Integer::sum);
                    titleCounts.merge(a.getTitle(), 1, Integer::sum);
                }
            }));
        }
        waitForFutures(futures);
        futures.clear();

        List<Future<List<Article>>> filterFutures = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            final int id = i;
            filterFutures.add(executor.submit(() -> {
                List<Article> localUnique = new ArrayList<>();
                int total = articles.size();
                int chunkSize = (int) Math.ceil((double) total / NUM_THREADS);
                int start = id * chunkSize;
                int end = Math.min(start + chunkSize, total);

                for (int j = start; j < end; j++) {
                    Article a = articles.get(j);
                    if (uuidCounts.get(a.getUuid()) == 1 && titleCounts.get(a.getTitle()) == 1) {
                        localUnique.add(a);
                    }
                }
                return localUnique;
            }));
        }

        List<Article> uniqueArticles = new ArrayList<>();
        for (Future<List<Article>> f : filterFutures) {
            try {
                uniqueArticles.addAll(f.get());
            } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        }
        filterFutures.clear();

        Article[] arr = uniqueArticles.toArray(new Article[0]);
        duplicatesCount -= arr.length;
        uniqueCount = arr.length;

        // Sort Articles by published date and uuid
        Sorter.parallelMergeSort(arr, executor, 0, arr.length - 1, NUM_THREADS);
        articles = Arrays.asList(arr);

        // Write all articles to output
        OutputWriter.writeAllArticles(articles);

        processedArticleIndex.set(0);

        // Process Articles
        List<Future<ProcessingResult>> processFutures = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            processFutures.add(executor.submit(() -> processArticlesDynamic(arr)));
        }

        for (Future<ProcessingResult> f : processFutures) {
            try {
                ProcessingResult res = f.get();
                res.localKeywords.forEach((k, v) -> keywordsCount.merge(k, v, Integer::sum));
                res.localAuthors.forEach((k, v) -> authorCounts.merge(k, v, Integer::sum));
                res.localCatCounts.forEach((k, v) ->
                        categoryCounts.computeIfAbsent(k, x -> new LongAdder()).add(v));
                res.localCatToUuids.forEach((k, v) ->
                        categoryToUuids.computeIfAbsent(k, x -> Collections.synchronizedList(new ArrayList<>())).addAll(v));
                res.localLangToUuids.forEach((k, v) ->
                        languageToUuids.computeIfAbsent(k, x -> Collections.synchronizedList(new ArrayList<>())).addAll(v));
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Processing Error: " + e.getMessage());
            }
        }
        processFutures.clear();

        if (!articles.isEmpty()) {
            mostRecentArticle = articles.get(0).getPublished().toString() + " " + articles.get(0).getUrl();
        }

        // Calculate tops
        calculateTops();

        // Write results and report
        OutputWriter.writeResults(executor, categoryToUuids, languageToUuids, keywordsCount, topKeyword, topKeywordCount);

        OutputWriter.writeReport(duplicatesCount, uniqueCount, bestAuthor, bestAuthorCount,
                topLanguage, topLanguageCount, topCategory, topCategoryCount,
                mostRecentArticle, topKeyword, topKeywordCount);

        executor.shutdown();
    }

    /**
     * Initialize parameters from command line arguments.
     * @param args
     */
    private static void init(String[] args) {
        try { NUM_THREADS = Integer.parseInt(args[0]); }
        catch (NumberFormatException e) { System.exit(1); }
        articlesPath = args[1];
        inputsPath = args[2];
    }

    /**
     * Wait for all futures to complete.
     * @param futures
     */
    private static void waitForFutures(List<Future<?>> futures) {
        for (Future<?> f : futures) {
            try { f.get(); }
            catch (InterruptedException | ExecutionException e) { System.err.println("Error: " + e.getMessage()); }
        }
    }

    /**
     * Calculate top author, category, language, and keyword.
     */
    private static void calculateTops() {
        for (Map.Entry<String, Integer> entry : authorCounts.entrySet()) {
            String author = entry.getKey();
            int count = entry.getValue();
            if (count > bestAuthorCount || (count == bestAuthorCount && author.compareTo(bestAuthor) < 0)) {
                bestAuthor = author;
                bestAuthorCount = count;
            }
        }

        for (Map.Entry<String, List<String>> entry : categoryToUuids.entrySet()) {
            String category = entry.getKey();
            int count = entry.getValue().size();
            if (count > topCategoryCount || (count == topCategoryCount && category.compareTo(topCategory) < 0)) {
                topCategory = category;
                topCategoryCount = count;
            }
        }

        for (Map.Entry<String, List<String>> entry : languageToUuids.entrySet()) {
            String lang = entry.getKey();
            int count = entry.getValue().size();
            if (count > topLanguageCount || (count == topLanguageCount && lang.compareTo(topLanguage) < 0)) {
                topLanguage = lang;
                topLanguageCount = count;
            }
        }

        for (Map.Entry<String, Integer> entry : keywordsCount.entrySet()) {
            if (entry.getValue() > topKeywordCount) {
                topKeyword = entry.getKey();
                topKeywordCount = entry.getValue();
            } else if (entry.getValue() == topKeywordCount) {
                if (topKeyword.isEmpty() || entry.getKey().compareTo(topKeyword) < 0) {
                    topKeyword = entry.getKey();
                }
            }
        }
    }

    /**
     * Process articles dynamically in batches.
     * @param articles
     * @return
     */
    private static ProcessingResult processArticlesDynamic(Article[] articles) {
        ProcessingResult localRes = new ProcessingResult();
        int total = articles.length;
        final int BATCH_SIZE = 50;

        while (true) {
            int start = processedArticleIndex.getAndAdd(BATCH_SIZE);
            if (start >= total) break;
            int end = Math.min(start + BATCH_SIZE, total);

            for (int i = start; i < end; i++) {
                Article art = articles[i];

                if (art.getCategories() != null) {
                    Set<String> uniqueCategories = new HashSet<>(art.getCategories());
                    for (String cat : uniqueCategories) {
                        localRes.localCatCounts.merge(cat, 1, Integer::sum);
                        localRes.localCatToUuids
                                .computeIfAbsent(cat, k -> new ArrayList<>())
                                .add(art.getUuid());
                    }
                }

                if (art.getLanguage() != null) {
                    localRes.localLangToUuids
                            .computeIfAbsent(art.getLanguage(), k -> new ArrayList<>())
                            .add(art.getUuid());
                }

                String text = art.getText();
                if (text != null && "english".equals(art.getLanguage())) {
                    text = TEXT_PATTERN.matcher(text.toLowerCase()).replaceAll("");

                    String[] words = text.split("\\s+");

                    Set<String> seenWords = new HashSet<>();
                    for (String word : words) {
                        if (!word.isBlank() && !englishLingingWords.contains(word)) {
                            seenWords.add(word);
                        }
                    }

                    for (String word : seenWords) {
                        localRes.localKeywords.merge(word, 1, Integer::sum);
                    }
                }

                String author = art.getAuthor();
                if (author != null && !author.isBlank()) {
                    localRes.localAuthors.merge(author, 1, Integer::sum);
                }
            }
        }
        return localRes;
    }
}