import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import software.amazon.awssdk.utils.IoUtils;


public class ReducerClass extends Reducer<Text, LongWritable, Text, DoubleWritable>{

    private HashMap<Long, Double> probabilityTable; // key = r, value = probability
    private Long[] rArray;

    @Override
    public void setup(Context context) throws IOException {

        try {
            Configuration configuration = new Configuration();
            String bucket = "yourfriendlyneighborhoodbucketman";
            FileSystem fileSystem = FileSystem.get(URI.create("s3://"+ bucket),context.getConfiguration());

            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("s3://" + bucket + "/stepTwo.txt"));
            String input = IoUtils.toUtf8String(fsDataInputStream);
            fsDataInputStream.close();
            fileSystem.close();
            customLoadProbabilityTable(input);
            makeRArray();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    private String removeLinesByPrefix(String totalInput){
        String newOutput = "";
        String[] arr = totalInput.split("\\n");
        for(int i=0 ;i< arr.length ; i++){
            if(arr[i] != null){
                newOutput += arr[i] + "\n";
                for(int j=1 ;j< arr.length;j++){
                    String[] subi = arr[i].split(" ");
                    String[] subj = arr[j].split(" ");
                    if(subi.length != 0 && subj.length != 0 )
                        if(subi[0].equals(subj[0]))
                            arr[j] = null;
                }
            }
        }
        return newOutput;
    }

    private String getKeyIndex(int count){
        String s1 = String.valueOf(count);
        int toAdd = 5-s1.length();
        for(int i=0 ;i<toAdd ; i++){
            s1 = "0" + s1;
        }
        return s1;
    }

    private String mergeHadoopFiles(Context context , FileSystem fileSystem, String bucket, String key) {
        try{
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("s3://" + bucket + "/" + key));
            String input = IoUtils.toUtf8String(fsDataInputStream);
            fsDataInputStream.close();
            return input;
        }catch (IOException e){

        }
        return null;
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;
        double probability;

        for (LongWritable value : values) {
            sum += value.get();
        }

        probability = calcProbability(sum);

        // <w1w2w3><probability>
        context.write(key, new DoubleWritable(probability));

    }

    private void customLoadProbabilityTable(String table){
        probabilityTable = new HashMap<>();
        String[] arr = table.split("\\n");

        long r;
        double probability;

        try {
            for(int i=0 ;i<arr.length ; i++){
                String line = arr[i];
                String[] lineReader = line.split("\\t");
                r = Long.parseLong(lineReader[0]);
                probability = Double.parseDouble(lineReader[1]);
                probabilityTable.put(r, probability);
            }
        }catch (Exception e){
            System.exit(1);
        }
    }

    private void loadProbabilityTable(){
        probabilityTable = new HashMap<>();

        long r;
        double probability;
        String[] lineReader;

        try {
            File probabilityTableFile = S3_Service.getInstance().downloadFromS3("probabilityTable");
            Scanner reader = new Scanner(probabilityTableFile);
            while(reader.hasNextLine()) {
                String line = reader.nextLine();
                lineReader = line.split("\\t");
                r = Long.parseLong(lineReader[0]);
                probability = Double.parseDouble(lineReader[1]);
                probabilityTable.put(r, probability);
            }
            reader.close();
        }
        catch (FileNotFoundException e) {
            System.out.println("ERROR: ReducerClass: loadProbabilityTable: cannot read from file!\n");
            e.printStackTrace();
        }
        S3_Service.getInstance().closeClient();
    }

    // used only for local runs
//    private void loadProbabilityTableLocal(){
//        probabilityTable = new HashMap<>();
//
//        long r;
//        double probability;
//        String[] lineReader;
//
//
//        try {
//            URL url = new URL("s3://yourfriendlyneighborhoodbucketman/probabilityTable.txt");
//            File probabilityTableFile = Paths.get(url.toURI()).toFile();
//            if(probabilityTableFile.length() == 0){
//                System.exit(1);
//            }
//            Scanner reader = new Scanner(probabilityTableFile);
//            while(reader.hasNextLine()) {
//                String line = reader.nextLine();
//                lineReader = line.split("\\t");
//                r = Long.parseLong(lineReader[0]);
//                probability = Double.parseDouble(lineReader[1]);
//                probabilityTable.put(r, probability);
//            }
//        }
//        catch (FileNotFoundException | MalformedURLException e) {
//            System.out.println("ERROR: ReducerClass: loadProbabilityTable: cannot read from file!\n");
//            e.printStackTrace();
//        } catch (URISyntaxException e) {
//            e.printStackTrace();
//        }
//    }

    private double calcProbability(long r){

        /*
            The function searches for r/2 instead of r because a flaw in the given formula of the task,
            which allows the option of searching r that is too big (because in previous steps we separated
            the corpus into 2 pieces).

            Also, the mentioned separation causes some r not to be in the probability table,
            because this time r is calculated on the whole corpus.
            The solution is to assume every 3-gram instances were divided equally between the 2 sub-corpus.
            This assumption is not the most accurate one, yet it's the best we can guarantee for now.
         */

        long searchedR = r/2;
        long closestValue;

        if(probabilityTable.containsKey(searchedR)){
            return probabilityTable.get(searchedR);
        }else{
            closestValue = getClosestValue(searchedR);
            if(probabilityTable.containsKey(closestValue))
                return probabilityTable.get(closestValue);
            else
                return 0.01;
        }
    }

    private void makeRArray() throws Exception {
        rArray = probabilityTable.keySet().toArray(new Long[0]);
        Arrays.sort(rArray);
        if(rArray.length ==0)
            throw new Exception("rArray is empty");
    }

    private long getClosestValue(long value){
        /*
            Binary search. By default, returns the location of the closest value in the array.
            Otherwise, it returns 0.
            Although the function searches for the value,
            it will not find it because it's called only if the value was not found.
         */

        int left = 0, right = rArray.length-1, mid = 0;

        while(left < right){
            mid = (left + right)/2;
            if(mid > rArray.length-1 || mid<=0)
                return 1;
            if(rArray[mid] == value){
                return mid;
            }
            if(rArray[mid] > value){
                right = mid - 1;
            }
            if(rArray[mid] < value){
                left = mid + 1;
            }
        }
        if(mid<0)
            mid = 0;
        else if(mid>rArray.length-1)
            mid = rArray.length-1;
        return rArray[mid];
    }


}
