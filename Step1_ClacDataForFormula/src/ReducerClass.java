import com.amazonaws.services.dynamodbv2.xspec.L;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable>{

    private long N;
    private HashMap<Long, Long> N0;
    private HashMap<Long, Long> N1;
    private HashMap<Long, Long> T01;
    private HashMap<Long, Long> T10;

    List<Long> corpus0;
    List<Long> corpus1;

    @Override
    public void setup(Context context){
        N = 0;
        N0 = new HashMap<>();
        N1 = new HashMap<>();
        T01 = new HashMap<>();
        T10 = new HashMap<>();

        corpus0 = new ArrayList<>();
        corpus1 = new ArrayList<>();
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context){
        long n = 0, r0 = 0, r1 = 0;

        corpusSeparation(values);

        r0 = sumList(corpus0);
        r1 = sumList(corpus1);
        n = r0 + r1;

        N += n;

        //update N0
        updateNI(N0, r0);

        //update N1
        updateNI(N1, r1);

        //update T01
        updateTI(T01, r0, r1);

        //update T10
        updateTI(T10, r1, r0);

        corpus0.clear();
        corpus1.clear();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        writeN(context);
        writeNI(context, N0, 0);
        writeNI(context, N1, 1);
        writeTI(context, T01, "01");
        writeTI(context, T10, "10");
    }

    /**
     * @return: true if corpus 0, false if corpus 1
     */
    private boolean determineCorpus(){
        Random random = new Random();

        int number = random.nextInt(100);

        return number%2 == 0;
    }

    private void corpusSeparation(Iterable<LongWritable> values){

        List<LongWritable> copy = copyInput(values);
        Random random = new Random();
        int corpus0Size = random.nextInt(copy.size()), index;

        for(int i=0; i<corpus0Size; i++){
            index = random.nextInt(copy.size());
            corpus0.add(copy.get(index).get());
            copy.remove(index);
        }

        for (LongWritable longWritable : copy) {
            corpus1.add(longWritable.get());
        }

    }

    private long sumList(List<Long> list){
        long sum = 0;

        for(Long element : list){
            sum += element;
        }

        return sum;

    }

    private List<LongWritable> copyInput(Iterable<LongWritable> values){
        List<LongWritable> copy = new ArrayList<>();

        for(LongWritable value : values){
            copy.add(value);
        }

        return copy;

    }

    private void updateNI(HashMap<Long, Long> map, long r){

        if(r == 0){
            return;
        }

        map.putIfAbsent(r, 0L);
        long nextRCount = map.get(r) + 1;
        map.replace(r, nextRCount);
    }

    private void updateTI(HashMap<Long, Long> map, long rIndex, long rValue){
        // rIndex: number of occurrences in current corpus
        // rValue: number of occurrences in the other corpus

        map.putIfAbsent(rIndex, 0L);
        long newVal = map.get(rIndex) + rValue;
        map.replace(rIndex, newVal);
    }

    private void writeN(Context context) throws IOException, InterruptedException {
        context.write(new Text("N"), new LongWritable(N));
    }

    private void writeNI(Context context, HashMap<Long, Long> map, int n_index) throws IOException, InterruptedException {
        // FORMAT: <N0 48><57> --> in N0, the number of 3-grams occurred 48 times is 57
        for(long r : map.keySet()){
            context.write(new Text("N" + n_index + " " + r), new LongWritable(map.get(r)));
        }
    }

    private void writeTI(Context context, HashMap<Long, Long> map, String n_index) throws IOException, InterruptedException {
        // FORMAT: <T01 48><57> --> in T01, the sum of 3-grams occurred 48 times is 57
        for(long r : map.keySet()){
            context.write(new Text("T" + n_index + " " + r), new LongWritable(map.get(r)));
        }
    }


}
