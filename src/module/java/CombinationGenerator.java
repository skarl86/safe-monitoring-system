import java.util.Arrays;
import java.util.Stack;

/**
 * Created by beggar3004 on 3/6/15.
 */
public class CombinationGenerator {

    public static final int[] BLANK_COMBINATION = new int[] { Integer.MIN_VALUE };

    private int k;
    private int n;
    private int[] result;
    private Stack<Integer> stack;
    private int[] source;
    private int[] next;

    public CombinationGenerator() {
        this.stack = new Stack<Integer>();
    }

    public void reset(int k, int[] source) {
        this.k = k;
        n = source.length;
        this.source = source;
        this.result = new int[k];
        this.stack.removeAllElements();
        this.stack.push(0);
        this.next = nextCombination();
        return;
    }

    public boolean hasNext() {
        if (k == 0 || BLANK_COMBINATION == next || !hasNextCombination())
            return false;
        return true;
    }

    public int[] next() {
        int[] result = this.next;
        this.next = nextCombination();
        Arrays.sort(this.next);

        return result;
    }

    private boolean hasNextCombination() {
        return this.stack.size() > 0;
    }

    private int[] nextCombination() {
        while (k != 0 && hasNextCombination()) {
            int index = this.stack.size() - 1;
            int value = this.stack.pop();

            while (value < n) {
                this.result[index++] = value++;
                this.stack.push(value);

                if (index == k) {
                    int[] combination = new int[k];
                    for (int i=0; i<k; i++)
                        combination[i] = this.source[this.result[i]];
                    return combination;
                }
            }
        }
        return BLANK_COMBINATION;
    }
}
