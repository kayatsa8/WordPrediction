import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    private final Text words = new Text();

    /**
     * @param key: <word1 word2 word3>
     * @param value: <probability>
     * @param context: context
     * @throws IOException
     * @throws InterruptedException
     *
     * output: <word1 word2 word3 probability><probability>
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String _words = getWords(value);
        double probability = getProbability(value);

        words.set(_words + " " + probability);

        context.write(words, new DoubleWritable(probability));

    }

    private String getWords(Text value){
        String input = value.toString();
        if(input == null || input.equals(""))
            return "";
        String[] _words = input.split("\\t");
        if(_words.length == 0)
            return "";
        return _words[0];
    }

    private double getProbability(Text value){
        String input = value.toString();
        String[] _words = input.split("\\t");
        return Double.parseDouble(_words[1]);
    }

}
