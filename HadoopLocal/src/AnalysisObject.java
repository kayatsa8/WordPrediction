import java.util.ArrayList;
import java.util.Comparator;

public class AnalysisObject{

    private String trio;
    private String[] firstInstances;
    private int instancesNumber;
    private double pdel;
    private static AnalysisComparator analysisComprator;

    public AnalysisObject(String trio, String[] firstInstances,int instancesNumber, double pdel){
        this.trio = trio;
        this.firstInstances = firstInstances;
        this.pdel = pdel;
        this.instancesNumber =instancesNumber;
        analysisComprator = new AnalysisComparator();
    }

    public double getPdel(){return this.pdel;}

    public int getInstancesNumber(){return this.instancesNumber;}

    public boolean greater(AnalysisObject a2){
        if(this.pdel >= a2.pdel)
            return true;
        else
            return false;
    }

    public Comparator<AnalysisObject> getComparator(){
        return analysisComprator;
    }

    public boolean addIfShould(ArrayList<AnalysisObject> arrAnalysisObjects){
        boolean status = false;
        for (AnalysisObject a: arrAnalysisObjects) {
            status = (this.greater(a)) || (a.instancesNumber < 4 && this.instancesNumber > a.instancesNumber);
            if(status){
                arrAnalysisObjects.remove(a);
                arrAnalysisObjects.add(this);
                arrAnalysisObjects.sort(analysisComprator);
                return true;
            }
        }
        return false;
    }

    public String toString(){
        String outputLine = trio;
        for(int i=instancesNumber ; i>=0 ; i--)
            outputLine+= " " + firstInstances[instancesNumber-i];
        outputLine+= " " + pdel;
        return outputLine;
    }

}

class AnalysisComparator implements Comparator<AnalysisObject> {
    @Override
    public int compare(AnalysisObject o1, AnalysisObject o2) {
        if(o1.getPdel() > o2.getPdel())
            return 1;
        else if(o1.getPdel() < o2.getPdel())
            return -1;
        else if(o1.getInstancesNumber() > o2.getInstancesNumber())
            return 1;
        else if(o1.getInstancesNumber() < o2.getInstancesNumber())
            return -1;
        else
            return 0;
    }
}
