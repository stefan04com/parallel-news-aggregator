import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class OutputWriter {

    /**
     * Writes all articles' UUIDs and published dates to "all_articles.txt".
     * @param articles
     */
    public static void writeAllArticles(List<Article> articles) {
        Path outputPath = Path.of("all_articles.txt");
        StringBuilder sb = new StringBuilder();
        for (Article art : articles) {
            sb.append(art.getUuid()).append(" ").append(art.getPublished()).append("\n");
        }
        try {
            Files.writeString(outputPath, sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            System.err.println("Could not write: " + e.getMessage());
        }
    }

    /**
     * Writes category, language, and keyword results to respective files in parallel.
     * @param executor
     * @param categoryToUuids
     * @param languageToUuids
     * @param keywordsCount
     * @param topKeyword
     * @param topKeywordCount
     */
    public static void writeResults(ExecutorService executor,
                                    Map<String, List<String>> categoryToUuids,
                                    Map<String, List<String>> languageToUuids,
                                    Map<String, Integer> keywordsCount,
                                    String topKeyword, int topKeywordCount) {
        List<Future<?>> writeFutures = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : categoryToUuids.entrySet()) {
            writeFutures.add(executor.submit(() -> {
                String category = entry.getKey();
                List<String> uuids = entry.getValue();
                Collections.sort(uuids);
                String fileName = category.replace(",", "").replaceAll("\\s+", "_") + ".txt";
                try {
                    Files.deleteIfExists(Path.of(fileName));
                    StringBuilder sb = new StringBuilder();
                    for (String uuid : uuids) sb.append(uuid).append("\n");
                    Files.writeString(Path.of(fileName), sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    System.err.println("Error writing category: " + fileName);
                }
            }));
        }

        for (Map.Entry<String, List<String>> entry : languageToUuids.entrySet()) {
            writeFutures.add(executor.submit(() -> {
                String lang = entry.getKey();
                List<String> uuids = entry.getValue();
                Collections.sort(uuids);
                String fileName = lang.toLowerCase() + ".txt";
                try {
                    Files.deleteIfExists(Path.of(fileName));
                    StringBuilder sb = new StringBuilder();
                    for (String uuid : uuids) sb.append(uuid).append("\n");
                    Files.writeString(Path.of(fileName), sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    System.err.println("Error writing language: " + fileName);
                }
            }));
        }

        List<String> keywords = new ArrayList<>(keywordsCount.keySet());
        keywords.sort((w1, w2) -> {
            int cmp = Integer.compare(keywordsCount.get(w2), keywordsCount.get(w1));
            return cmp != 0 ? cmp : w1.compareTo(w2);
        });

        try {
            Path keywordPath = Path.of("keywords_count.txt");
            Files.deleteIfExists(keywordPath);
            StringBuilder sb = new StringBuilder();
            for (String word : keywords) {
                int count = keywordsCount.get(word);
                sb.append(word).append(" ").append(count).append("\n");
            }
            Files.writeString(keywordPath, sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {}

        for (Future<?> f : writeFutures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Error in parallel write: " + e.getMessage());
            }
        }
    }

    /**
     * Writes the final report to "reports.txt".
     * @param duplicatesCount
     * @param uniqueCount
     * @param bestAuthor
     * @param bestAuthorCount
     * @param topLanguage
     * @param topLanguageCount
     * @param topCategory
     * @param topCategoryCount
     * @param mostRecentArticle
     * @param topKeyword
     * @param topKeywordCount
     */
    public static void writeReport(int duplicatesCount, int uniqueCount,
                                   String bestAuthor, int bestAuthorCount,
                                   String topLanguage, int topLanguageCount,
                                   String topCategory, int topCategoryCount,
                                   String mostRecentArticle,
                                   String topKeyword, int topKeywordCount) {
        try {
            Path reportPath = Path.of("reports.txt");
            Files.deleteIfExists(reportPath);
            StringBuilder sb = new StringBuilder();
            sb.append("duplicates_found - ").append(duplicatesCount).append("\n");
            sb.append("unique_articles - ").append(uniqueCount).append("\n");
            sb.append("best_author - ").append(bestAuthor).append(" ").append(bestAuthorCount).append("\n");
            sb.append("top_language - ").append(topLanguage).append(" ").append(topLanguageCount).append("\n");
            String newcatFinal = topCategory.replace(",", "").replaceAll("\\s+", "_");
            sb.append("top_category - ").append(newcatFinal).append(" ").append(topCategoryCount).append("\n");
            sb.append("most_recent_article - ").append(mostRecentArticle).append("\n");
            sb.append("top_keyword_en - ").append(topKeyword).append(" ").append(topKeywordCount).append("\n");
            Files.writeString(reportPath, sb.toString(), StandardOpenOption.CREATE);
        } catch (IOException e) {}
    }
}