import java.util.ArrayList;
import java.io.*;

public class ex_merge {

    static void merge(int top_k, int num_partitions) throws Exception {
        System.out.println("Top K: " + top_k + "\n");
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
        System.out.println("finished.\n");
    }

    public static void main(String[] args) throws Exception {
        merge(50000, 205);
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
