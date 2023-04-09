import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable lineID, Text text,Context context) throws IOException, InterruptedException {
        String toSplit = text.toString();
        if(toSplit.length() != 0){
            Text key = decodeKey(toSplit);
            LongWritable value = decodeValue(toSplit);
            if(value != null)
                context.write(key,value);
        }
    }

    private static Text decodeKey(String text){
        //Step 1 emitted: either context.write(new Text("N"), new LongWritable(N)); or context.write(new Text("N" + n_index + " " + r), new LongWritable(map.get(r)));
        String[] afterSplit = text.split("\\t"); //size of 2
        return new Text(afterSplit[0]);
    }

    private static LongWritable decodeValue(String text){
        //Step 1 emitted: either context.write(new Text("N"), new LongWritable(N)); or context.write(new Text("N" + n_index + " " + r), new LongWritable(map.get(r)));
        String[] afterSplit = text.split("\\t"); //size of 2
        if(afterSplit.length <2)
            return null;
        return new LongWritable(Long.parseLong(afterSplit[1]));
    }

}
