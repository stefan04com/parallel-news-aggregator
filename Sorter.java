import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Sorter {

    /**
     * Perform a parallel merge sort on the array of Articles.
     * @param arr
     * @param executor
     * @param left
     * @param right
     * @param numThreads
     */
    public static void parallelMergeSort(Article[] arr, ExecutorService executor, int left, int right, int numThreads) {
        int n = right - left + 1;
        Article[] aux = new Article[arr.length];

        if (numThreads <= 1 || n < 100) {
            sequentialMergeSort(arr, aux, left, right);
            return;
        }

        List<Future<?>> futures = new ArrayList<>();

        int chunkSize = (int) Math.ceil((double) n / numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int chunkStart = left + i * chunkSize;
            final int chunkEnd = Math.min(chunkStart + chunkSize - 1, right);

            if (chunkStart <= chunkEnd) {
                futures.add(executor.submit(() -> {
                    sequentialMergeSort(arr, aux, chunkStart, chunkEnd);
                }));
            }
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                Arrays.sort(arr, left, right + 1, comparator());
                return;
            }
        }

        int currentSize = chunkSize;
        while (currentSize < n) {
            for (int i = left; i <= right - currentSize; i += 2 * currentSize) {
                int mid = i + currentSize - 1;
                int end = Math.min(i + 2 * currentSize - 1, right);
                merge(arr, aux, i, mid, end);
            }
            currentSize *= 2;
        }
    }

    /**
     * Perform a sequential merge sort on the subarray arr[left..right].
     * @param arr
     * @param aux
     * @param left
     * @param right
     */
    private static void sequentialMergeSort(Article[] arr, Article[] aux, int left, int right) {
        if (left >= right) return;

        int mid = (left + right) / 2;
        sequentialMergeSort(arr, aux, left, mid);
        sequentialMergeSort(arr, aux, mid + 1, right);
        merge(arr, aux, left, mid, right);
    }

    /**
     * Merge two sorted subarrays of arr using aux as auxiliary storage.
     * @param arr
     * @param aux
     * @param left
     * @param mid
     * @param right
     */
    private static void merge(Article[] arr, Article[] aux, int left, int mid, int right) {
        System.arraycopy(arr, left, aux, left, right - left + 1);

        int i = left;
        int j = mid + 1;
        int k = left;

        Comparator<Article> comp = comparator();

        while (i <= mid && j <= right) {
            if (comp.compare(aux[i], aux[j]) <= 0) {
                arr[k++] = aux[i++];
            } else {
                arr[k++] = aux[j++];
            }
        }

        while (i <= mid) {
            arr[k++] = aux[i++];
        }

        while (j <= right) {
            arr[k++] = aux[j++];
        }
    }

    /**
     * Comparator for Articles: first by published date (descending), then by UUID (ascending).
     * @return
     */
    private static Comparator<Article> comparator() {
        return (a1, a2) -> {
            int cmp = a2.getPublished().compareTo(a1.getPublished());
            if (cmp != 0) return cmp;
            return a1.getUuid().compareTo(a2.getUuid());
        };
    }
}