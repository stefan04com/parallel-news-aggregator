import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class DataLoader {

    /**
     * Reads article paths from a specified file.
     * @param articlesPath
     * @return
     */
    public static String[] readPaths(String articlesPath) {
        Path articlesFilePath = Path.of(articlesPath);
        Path baseDirectory = articlesFilePath.getParent();
        List<String> lines;
        try {
            lines = Files.readAllLines(articlesFilePath);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
            return null;
        }

        int numArticles = Integer.parseInt(lines.get(0).trim());
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= numArticles; i++) {
            String relativePath = lines.get(i).trim();
            if (!relativePath.isEmpty()) {
                Path articlePath = baseDirectory.resolve(relativePath).normalize();
                list.add(articlePath.toString());
            }
        }
        return list.toArray(new String[0]);
    }

    /**
     * Reads article files dynamically from a queue.
     * @param objectMapper
     * @param fileQueue
     * @return
     */
    public static List<Article> readFilesDynamic(ObjectMapper objectMapper, Queue<String> fileQueue) {
        List<Article> localArticles = new ArrayList<>();
        String filePath;

        while ((filePath = fileQueue.poll()) != null) {
            try {
                Path path = Path.of(filePath);
                String jsonContent = Files.readString(path);
                JsonParser parser = objectMapper.getFactory().createParser(jsonContent);

                while (parser.nextToken() != null) {
                    if (parser.getCurrentToken() == com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                        Article art = objectMapper.readValue(parser, Article.class);
                        localArticles.add(art);
                    }
                }
                parser.close();
            } catch (Exception e) {
                System.err.println("Error reading file " + filePath + ": " + e.getMessage());
            }
        }
        return localArticles;
    }

    /**
     * Reads chunked inputs for languages, categories, or English linking words.
     * @param path
     * @param id
     * @param numThreads
     * @param lines
     * @param N
     * @param lower
     * @param languages
     * @param categories
     * @param englishLinkingWords
     */
    public static void readChunkInputs(String path, int id, int numThreads, List<String> lines, int N, String lower,
                                       String[] languages, String[] categories, Set<String> englishLinkingWords) {
        List<String> elems = lines.subList(1, lines.size());

        boolean isLanguages = lower.contains("languages");
        boolean isCategories = lower.contains("categories");
        boolean isLinking = lower.contains("english_linking");

        if (!isLanguages && !isCategories && !isLinking) return;

        int chunkSize = (int) Math.ceil((double) N / numThreads);
        int start = id * chunkSize;
        int end = Math.min(start + chunkSize, N);

        for (int i = start; i < end; i++) {
            String val = elems.get(i);
            if (isLanguages) {
                languages[i] = val;
            } else if (isCategories) {
                categories[i] = val;
            } else if (isLinking) {
                englishLinkingWords.add(val);
            }
        }
    }
}