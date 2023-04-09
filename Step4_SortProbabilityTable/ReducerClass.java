import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

public class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

        DoubleWritable probability = values.iterator().next(); // possible because there will be only 1 pair with key <W1 W2 W3 P>, therefore 1 value

        Text remadeKey = remakeKey(key);

        // <w1 w2 w3><probability>
        context.write(remadeKey, probability);
    }

    private Text remakeKey(Text key){
        String[] currKey = key.toString().split(" ");
        return new Text(currKey[0] + " " + currKey[1] + " " + currKey[2]);
    }

}
