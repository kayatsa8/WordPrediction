import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MapperClass  extends Mapper<LongWritable, Text, Text, LongWritable>{

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
     * output: <word1 word2 word3><1>
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String stringValue = value.toString();
        if(stringValue.length() != 0){
            String _words = getWords(stringValue);
            if(!inputValidation(_words) || stopWord(_words)){
                return;
            }
            words.set(_words);
            context.write(words, ONE);
        }
    }

    private String getWords(String ngramInput){
        String[] afterSplit = ngramInput.split("\t");
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

    private boolean inputValidation(String words){
        String[] _words = words.split(" ");

        return _words.length == 3;
    }


}
