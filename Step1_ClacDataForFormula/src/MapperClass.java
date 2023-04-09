import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable>{

    private final static LongWritable ONE = new LongWritable(1);
    private Text words = new Text();
    private final int nGramPosition = 0;
    private List<String> stopWords;

    @Override
    public void setup(Context context){
        loadStopWords();
    }

    /**
     * @param key: lineId by google ngram
     * @param value: string in the format of google
     * @param context: context
     * @throws IOException
     * @throws InterruptedException
     *
     * output: <word1 word2 word3><num of occurrences>
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(value == null)
            return ;
        String _words = getWords(value.toString());
        if(!inputValidation(_words) || stopWord(_words)){
            return;
        }

        words.set(_words);

        context.write(words, ONE);

    }

    private boolean inputValidation(String words){
        String[] _words = words.split(" ");

        return _words.length == 3;
    }

    private String getWords(String ngramInput){
        String[] afterSplit = ngramInput.split("\\t");
        if(afterSplit.length == 0)
            return "";
        return afterSplit[nGramPosition];
    }

    private boolean stopWord(String words){
        String[] _words = words.split(" ");
        return isStopWord(_words[0]) || isStopWord(_words[1]);
    }

    private boolean isStopWord(String word){
        for(String stopWord : stopWords){
            if(stopWord.equals(word)){
                return true;
            }
        }
        return false;
    }

    private void loadStopWords(){
        stopWords = new ArrayList<>();

        try {
            File file = new File("eng-stopwords.txt");
            Scanner reader = new Scanner(file);
            while (reader.hasNextLine()) {
                String line = reader.nextLine();
                stopWords.add(line);
            }
            reader.close();
        }
        catch (FileNotFoundException e) {
            System.out.println("ERROR: MapperClass: loadStopWords: cannot read from file!\n");
            e.printStackTrace();
        }

    }

}
