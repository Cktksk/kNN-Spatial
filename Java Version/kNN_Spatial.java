import java.io.*;
import java.util.*;
import java.awt.geom.Point2D;

public class kNN_Spatial {
    static long start;
    // final static long limit = -1; //Limit the input size
    static int k = 5000;
    final static int bucket_size = 100;
    final static int run_size = 1000;
    static int num_partition = 0;
    static int totalBsize = 0;
    static Double cutoff = null;
    static PriorityQueue<List<Double>> pq;
    static Map<Double, List<int[]>> dict;

    private static void merge(int top_k, int num_partitions) throws Exception {
        System.out.println("Top K: " + top_k);
        int k = num_partitions;
        ArrayList<MinHeapNode> h_arr = new ArrayList<MinHeapNode>();
        BufferedWriter fout = new BufferedWriter(new FileWriter("output"));
        BufferedReader[] fin = new BufferedReader[k];
        int i;
        for (i = 0; i < k; i++) {
            String path = "tmp/out" + String.valueOf(i);
            fin[i] = new BufferedReader(new FileReader(path));
        }
        for (i = 0; i < k; i++) {
            String line = fin[i].readLine();
            MinHeapNode node = new MinHeapNode(Double.parseDouble(line), i);
            h_arr.add(node);
        }

        MinHeap min_heap = new MinHeap(h_arr, k);
        int count = 0;
        int curr_index = 0;
        int current_k = 0;
        while (count < k) {
            MinHeapNode root = min_heap.get_min();
            curr_index = root.index;
            fout.write(String.valueOf(root.distance));
            fout.newLine();
            current_k += 1;
            if (current_k == top_k)
                break;
            String next_line = fin[curr_index].readLine();
            if (next_line != null) {
                root.distance = Double.parseDouble(next_line);
                root.index = curr_index;
            } else {
                count += 1;
                root.distance = Double.MAX_VALUE;
            }
            min_heap.replace_min(root);
        }
        fout.close();
        for (i = 0; i < k; i++)
            fin[i].close();

    }

    private static void run_generation_spatial() throws IOException {
        pq = new PriorityQueue<>((arr1, arr2) -> Double.compare(arr2.get(0), arr1.get(0)));
        dict = new HashMap<>();
        List<Double> chunk = new ArrayList<>();
        int[] target = new int[]{0, 0};
        int counter = 0;
        int row_count = 0;
        List<int[]> input = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("spatial_data"))) {
            String line;
            while ((line = br.readLine()) != null) {
                int i = Integer.parseInt(line.split(",")[0]);
                int j = Integer.parseInt(line.split(",")[1]);
                input.add(new int[]{i, j});
            }
        }
        start = System.currentTimeMillis(); // Start counting
        int input_ate = 0;
        for (int[] points : input) {
            double n = (target[1] - points[1]) * (target[1] - points[1]) + (target[0] - points[0]) * (target[0] - points[0]);
            n = customRound(n, 4);
            if (cutoff != null && n > cutoff) continue; // Invalid Input, ignore and continue

            if (!dict.containsKey(n)) {
                dict.put(n, new ArrayList<>());
            }
            dict.get(n).add(new int[]{points[0], points[1]});

            if (chunk.size() < run_size) {
                chunk.add(n);
            }

            if (chunk.size() == run_size) {
                chunk.sort(Collections.reverseOrder());
                List<List<Double>> buckets = split_bucket(chunk);
                List<Double> writeOut = insert2queue(buckets);
                Collections.reverse(writeOut);
                FileWriter fw = new FileWriter("tmp/out" + counter);
                for (double item : writeOut) {
                    fw.write(String.valueOf(item) + System.getProperty("line.separator"));
                    row_count++;
                }
                fw.close();
                counter++;
                num_partition++;
                chunk.clear();
//                System.out.printf("Cufoff Key: %f" + System.getProperty("line.separator"), cutoff);
//                System.out.println("------------------------------");
            }
        }
        if (chunk.size() > 0) {
            chunk.sort(Collections.reverseOrder());
            List<List<Double>> buckets = split_bucket(chunk);
            List<Double> writeOut = insert2queue(buckets);
            Collections.reverse(writeOut);
            FileWriter fw = new FileWriter("tmp/out" + counter);
            for (double item : writeOut) {
                fw.write(String.valueOf(item) + System.getProperty("line.separator"));
                row_count++;
            }
            fw.close();
            num_partition++;
        }
        System.out.printf("Number of Rows: %d\n", row_count);
        System.out.printf("Number of Runs: %d\n", counter + 1);
    }

    private static List<Double> insert2queue(List<List<Double>> buckets) {
        List<Double> output = new ArrayList<>();
        for (List<Double> x : buckets) {
            totalBsize += x.size();
            double x_key = x.get(0);
            pq.add(new ArrayList<>(x));
            output.addAll(x);
            if (totalBsize > k) {
                if (cutoff != null) pq.poll();
                cutoff = pq.peek().get(0);
            }
        }
        return output;
    }

    private static List<List<Double>> split_bucket(List<Double> chunk) {
        if (chunk == null) return null;
        List<List<Double>> buckets = new ArrayList<>();
        for (int start = 0; start < chunk.size(); start += bucket_size) {
            int end = Math.min(start + bucket_size, chunk.size());
            List<Double> tmp = chunk.subList(start, end);
            buckets.add(new ArrayList<>(tmp));
        }
        return buckets;
    }

    private static double customRound(double n, int place) {
        double scale = Math.pow(10, place);
        return Math.round(n * scale) / scale;
    }

    public static void main(String[] args) throws Exception {
        File dir = new File("tmp/");
        if (!dir.exists()) dir.mkdir();
        String[] entries = dir.list();
        for (String s : entries) {
            File currentFile = new File(dir.getPath(), s);
            currentFile.delete();
        }

        Scanner myObj = new Scanner(System.in);
        System.out.println("Enter Top K:");
        k = Integer.parseInt(myObj.nextLine());
        run_generation_spatial();
        merge(k, num_partition);
        try (BufferedReader br = new BufferedReader(new FileReader("output"))) {
            FileWriter fw = new FileWriter("Revised_Output");
            String line;
            Map<Double, Integer> seen = new HashMap<>();
            while ((line = br.readLine()) != null) {
                double key = Double.parseDouble(line);
                if (!seen.containsKey(key)) seen.put(key, 0);
                else seen.put(key, seen.get(key) + 1);
                double s_key = Math.sqrt(key);
                fw.write(String.valueOf(s_key) + ": " + Arrays.toString(dict.get(key).get(seen.get(key))) + System.getProperty("line.separator"));
            }
            fw.close();
        }

        // System.out.println("finished.");
        System.out.printf("Cutoff: %f\n", cutoff/1000000.0);
        double elapsedTime = (System.currentTimeMillis() - start) / 1000.0;
        System.out.printf("Total runtime: %fs\n", elapsedTime);
    }
}

class MinHeapNode {
    double distance;
    int index;

    MinHeapNode(MinHeapNode mhn) {
        this.distance = mhn.distance;
        this.index = mhn.index;
    }

    MinHeapNode(double distance, int index /* index of array taken */) {
        this.distance = distance;
        this.index = index;
    }
}

class MinHeap {
    int heap_size;
    ArrayList<MinHeapNode> heap_arr;

    MinHeap() {
        heap_arr = new ArrayList<MinHeapNode>();
    }

    MinHeap(ArrayList<MinHeapNode> ar, int size) {
        heap_size = size;
        heap_arr = ar;
        int i = (heap_size - 1) / 2;
        while (i >= 0) {
            min_heapify(i);
            i -= 1;
        }
    }

    void swap(ArrayList<MinHeapNode> ar, int i, int j) {
        MinHeapNode tmp = new MinHeapNode(ar.get(i));
        ar.set(i, ar.get(j));
        ar.set(j, tmp);
    }

    void min_heapify(int i) {
        int l = 2 * i + 1;// left
        int r = 2 * i + 2; // right
        int smallest = i;
        if (l < heap_size && heap_arr.get(l).distance < heap_arr.get(i).distance)
            smallest = l;
        if (r < heap_size && heap_arr.get(r).distance < heap_arr.get(smallest).distance)
            smallest = r;
        if (smallest != i) {
            swap(heap_arr, i, smallest);
            min_heapify(smallest);
        }
    }

    MinHeapNode get_min() {
        if (heap_size <= 0) {
            System.out.println("Heap underflow");
            return null;
        }
        return heap_arr.get(0);
    }

    void replace_min(MinHeapNode root) {
        heap_arr.set(0, root);
        min_heapify(0);
    }
}

