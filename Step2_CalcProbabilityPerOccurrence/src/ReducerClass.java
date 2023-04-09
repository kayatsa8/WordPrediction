import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ReducerClass extends Reducer<Text, LongWritable, LongWritable, DoubleWritable> {

    /*
    input options: <"N",count> , <"N0 index", count> , <"N1 index", count> , <"T01 index", count> , <"T10 index", count>
    output: <r,pdel> where r is index and pdel is the needed probability
    index,count are longs.
    * */

    private long N;
    private HashMap<Long,Long> N0,N1,T01,T10;
    private int varPosition,indexPosition;

    @Override
    public void setup(Context context){
        N = 0;
        N0 = new HashMap<>();
        N1 = new HashMap<>();
        T01 = new HashMap<>();
        T10 = new HashMap<>();
        varPosition = 0;
        indexPosition = 1;
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> counts, Context context){
        long sum =0;
        //step 1
        for (LongWritable value: counts) {
            //length of one
            sum += value.get();
        }
        //step 2
        if(key.toString().split(" ").length == 1) //key is "N"
            N += sum;
        else{
            //updating sum in appropriate table: key = index ; value = sum
            Object[] arr = getMap(key.toString());
            HashMap<Long,Long> map = (HashMap<Long, Long>) arr[varPosition];
            long index = Long.parseLong((String) arr[indexPosition]);
            map.put(index,sum);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        /*assumptions:
            1)if index0 is in N0 then it must be on T01
            2)if index1 is in N1 then it must be on T10
            3)conclusion:if index is in both N0 and N1 then it's both in T01 and T10
        * */
        double pdel;

        Set<Long> n0Keys = new HashSet<>(N0.keySet());

        long index01,index10;
        for (long index0 : n0Keys) {
            boolean same = false;
            long index1;
            if(N1.keySet().size() == 0){
                index1 = -1;
                index01 = getCloseIndex(T01,index0);
                index10 = getCloseIndex(T10,index0);
            }
            else{
                index1 = getCloseIndex(N1,index0);
                index01 = getCloseIndex(T01,index0);
                index10 = getCloseIndex(T10,index1);
                if(index0 == index1)
                    same = true;
            }
            pdel = computePdel(index0,index1,index01,index10);
            context.write(new LongWritable((index0)),new DoubleWritable(pdel));
            if(same){
                N0.remove(index0);
                N1.remove(index1);
            }
        }
        for (long index1 : N1.keySet()){
            long index0;
            if(N0.keySet().size() == 0){
                index0 = -1;
                index01 = getCloseIndex(T01,index1);
                index10 = getCloseIndex(T10,index1);
            }else{
                index0 = getCloseIndex(N0,index1);
                index01 = getCloseIndex(T01,index0);
                index10 = getCloseIndex(T10,index1);
            }
            pdel = computePdel(index0,index1,index01,index10);
            context.write(new LongWritable((index1)),new DoubleWritable(pdel));
        }
    }

    //cleanup help functions.
    private double computePdel(long index0, long index1,long index01,long index10){
        double n = N;
        double n0 = N0.get(index0);
        double n1 = N1.get(index1);
        double t01 = T01.get(index0);
        double t10 = T10.get(index1);
        return (t01+t10)/(n*(n0+n1));
    }

    private long getCloseIndex(HashMap<Long,Long> table,long index){
        /*
            Binary search. By default, returns the location of the closest value in the array.
            Otherwise, it returns 0.
            Although the function searches for the value,
            it will not find it because it's called only if the value was not found.
         */
        int left = 0, right = table.size()-1, mid = 0;
        Long[] iArray = table.keySet().toArray(new Long[0]);
        Arrays.sort(iArray);
        if(iArray.length ==0)
            return -1;
        while(left < right){
            mid = (left + right)/2;
            if(iArray[mid] == index){
                return mid;
            }
            if(iArray[mid] > index){
                right = mid - 1;
            }
            if(iArray[mid] < index){
                left = mid + 1;
            }
        }
        return iArray[mid];
    }

    //reduce help functions
    private Object[] getMap(String key){
        String[] afterSplit = key.split(" ");
        String tableName = afterSplit[varPosition];
        if(tableName.equals("N0"))
            return new Object[]{N0,afterSplit[indexPosition]};
        else if(tableName.equals("N1"))
            return new Object[]{N1,afterSplit[indexPosition]};
        else if(tableName.equals("T01"))
            return new Object[]{T01,afterSplit[indexPosition]};
        else
            return new Object[]{T10,afterSplit[indexPosition]};
    }



}
