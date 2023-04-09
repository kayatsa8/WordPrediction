import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NewOrder extends WritableComparator{

    protected NewOrder(){
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        return determineFirst((Text)w1, (Text)w2);
    }

    private int determineFirst(Text key1, Text key2){
        String[] k1 = key1.toString().split(" ");
        String[] k2 = key2.toString().split(" ");

        int firstCompare = k1[0].compareTo(k2[0]);
        if(firstCompare != 0){
            return firstCompare;
        }

        int secondCompare = k1[1].compareTo(k2[1]);
        if(secondCompare != 0){
            return secondCompare;
        }

        double thirdCompare = 1;
        if(k1[3] != null && k1[3].length() >0)
            if(k2[3] != null && k2[3].length() > 0)
                thirdCompare = Double.parseDouble(k1[3]) - Double.parseDouble(k2[3]);

        if(thirdCompare < 0){
            return 1;
        }
        else{
            return -1;
        }

    }

}
