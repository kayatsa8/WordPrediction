import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {

    private static EmrClient emr = EmrClient.builder().credentialsProvider(ProfileCredentialsProvider.create()).build();

    public static void main(String[] args) throws InterruptedException {

        uploadFile("Step1.jar");
        uploadFile("Step2.jar");
        uploadFile("Step3.jar");
        uploadFile("Step4.jar");
        String stepName;
        String folderName;
        String inputArg = "";

        JobFlowInstancesConfig jobFlowInstancesConfig = JobFlowInstancesConfig.builder()
                .instanceCount(8)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("5.36.0")
                .ec2KeyName("vockey")
                .keepJobFlowAliveWhenNoSteps(true)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build()).build();

        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("hadoopmenicourse")
                .instances(jobFlowInstancesConfig)
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.36.0")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("LabInstanceProfile")
                .logUri("s3://yourfriendlyneighborhoodbucketman/logs/")
                .build();
        RunJobFlowResponse runJobFlowResponse = emr.runJobFlow(runFlowRequest);
        String clusterId; //cluster id = jobflow id
        do{
            clusterId = runJobFlowResponse.jobFlowId();
            try{
                TimeUnit.SECONDS.sleep(2);
            }
            catch(Exception e){
                System.err.println("ERROR:\n");
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }while (clusterId == null);
        System.out.println("ClusterId = " + clusterId);


        //step1
        HadoopJarStepConfig hadoopJarStepConfig1 = HadoopJarStepConfig.builder()
                .jar("s3://yourfriendlyneighborhoodbucketman/Step1.jar") // This should be a full map
                .mainClass("Main")
                .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/3gram/data", "s3://yourfriendlyneighborhoodbucketman/somefile.txt").build();
        StepConfig stepConfig1 = StepConfig.builder()
                .name("step1")
                .actionOnFailure("CONTINUE") //TERMINATE_JOB_FLOW
                .hadoopJarStep(hadoopJarStepConfig1).build();
        AddJobFlowStepsResponse step1Response= emr.addJobFlowSteps(AddJobFlowStepsRequest.builder()
                .steps(stepConfig1)
                .jobFlowId(clusterId)
                .build());
        if(checkStepTerminated(step1Response,clusterId)){
            HashMap<String,Long> table1 = new HashMap<>();
            stepName = "stepOne";
            folderName = "somefile.txt";
            combineOutputsFromStepLong(folderName,stepName,table1);
            uploadFile(stepName + ".txt");
            inputArg = "s3://yourfriendlyneighborhoodbucketman/" + stepName + ".txt";
        } else
            System.exit(1);

        //step2
        HadoopJarStepConfig hadoopJarStepConfig2 = HadoopJarStepConfig.builder()
                .jar("s3://yourfriendlyneighborhoodbucketman/Step2.jar") // This should be a full map
                .mainClass("Main")
                .args(inputArg, "s3://yourfriendlyneighborhoodbucketman/probabilityTable.txt").build();
        StepConfig stepConfig2 = StepConfig.builder()
                .name("step2")
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .hadoopJarStep(hadoopJarStepConfig2).build();
        AddJobFlowStepsResponse step2Response= emr.addJobFlowSteps(AddJobFlowStepsRequest.builder()
                .steps(stepConfig2)
                .jobFlowId(clusterId)
                .build());
        if(checkStepTerminated(step2Response,clusterId)){
            HashMap<String,Double> table2 = new HashMap<>();
            stepName = "stepTwo";
            folderName = "probabilityTable.txt";
            combineOutputsFromStepDouble(folderName,stepName,table2);
            uploadFile(stepName + ".txt");
        } else
            System.exit(1);
        //step3
        HadoopJarStepConfig hadoopJarStepConfig3 = HadoopJarStepConfig.builder()
                .jar("s3://yourfriendlyneighborhoodbucketman/Step3.jar") // This should be a full map
                .mainClass("Main")
                .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/3gram/data","s3://yourfriendlyneighborhoodbucketman/step4Input.txt").build();
        StepConfig stepConfig3 = StepConfig.builder()
                .name("step3")
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .hadoopJarStep(hadoopJarStepConfig3).build();
        AddJobFlowStepsResponse step3Response= emr.addJobFlowSteps(AddJobFlowStepsRequest.builder()
                .steps(stepConfig3)
                .jobFlowId(clusterId)
                .build());
        if(checkStepTerminated(step3Response,clusterId)){
            HashMap<String,Double> table3 = new HashMap<>();
            stepName = "stepThree";
            folderName = "step4Input.txt";
            combineOutputsFromStepDouble(folderName,stepName,table3);
            uploadFile(stepName + ".txt");
            inputArg = "s3://yourfriendlyneighborhoodbucketman/" + stepName + ".txt";
        } else
            System.exit(1);
        //step4
        HadoopJarStepConfig hadoopJarStepConfig4 = HadoopJarStepConfig.builder()
                .jar("s3://yourfriendlyneighborhoodbucketman/Step4.jar") // This should be a full map
                .mainClass("some.pack.MainClass")
                .args(inputArg, "s3://yourfriendlyneighborhoodbucketman/triGramProbability.txt").build();
        StepConfig stepConfig4 = StepConfig.builder()
                .name("step4")
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .hadoopJarStep(hadoopJarStepConfig4).build();

        AddJobFlowStepsResponse step4Response= emr.addJobFlowSteps(AddJobFlowStepsRequest.builder()
                .steps(stepConfig4)
                .jobFlowId(clusterId)
                .build());

        if(checkStepTerminated(step4Response,clusterId)){
            emr.terminateJobFlows(TerminateJobFlowsRequest.builder().jobFlowIds(clusterId).build());
            try {
                if (true) {
                    File triGramProbabilityFile = downloadTriGramProbabilityFile();
                    System.out.println(triGramProbabilityFile.getPath());
                    analysisOutputFile(analysis(triGramProbabilityFile));
                    statistics();
                }
            }catch (IOException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }

    private static boolean checkStepTerminated(AddJobFlowStepsResponse stepResponse , String clusterId) throws InterruptedException {
        List<String> id;
        boolean stop = false;
        DescribeStepResponse res;
        StepStatus status;
        StepState stepState = null;
        String state = null;
        do{
            TimeUnit.SECONDS.sleep(60);
            id = stepResponse.stepIds();
            if(id.size() != 0){
                res = emr.describeStep(DescribeStepRequest.builder().clusterId(clusterId).stepId(id.get(0)).build());
                status = res.step().status();
                stepState = status.state();
                state = stepState.name();
                stop = (state.equals("COMPLETED") || state.equals("FAILED") || state.equals("INTERRUPTED"));
            }
        }while(!stop);
        if(state.equals("COMPLETED")){
            System.out.println("checkStepTerminated: true" + stepState.name());
            return true;
        }
        else
            return false;
    }

    private static void combineOutputsFromStepLong(String folderName, String stepName, HashMap<String, Long> table){
        /*
        1.download all files
        2.merge them - same prefix need to be merged together.
        3.upload the new file to s3 - in current path we have stepName.txt combined file of all step's results.
        * */

        String key;
        File file = null;
        int counter = 0;
        do{
            key = getKeyIndex(counter);
            key = folderName + "/part-r-" + key;
            S3_Service.getInstance().makeKey(key);
            file = S3_Service.getInstance().downloadFromS3(stepName);
            if(file == null)
                break;
            mergeFilesLong(file,table);
            counter++;
            System.out.println(table.keySet().size());
        }while(file != null);
        writeOutputStepFileLong(stepName,table);
    }

    private static void combineOutputsFromStepDouble(String folderName , String stepName,HashMap<String,Double> table){
        /*
        1.download all files
        2.merge them - same prefix need to be merged together.
        3.upload the new file to s3 - in current path we have stepName.txt combined file of all step's results.
        * */

        String key;
        File file = null;
        int counter = 0;
        do{
            key = getKeyIndex(counter);
            key = folderName + "/part-r-" + key;
            S3_Service.getInstance().makeKey(key);
            file = S3_Service.getInstance().downloadFromS3(stepName);
            if(file == null)
                break;
            mergeFilesDouble(file,table);
            counter++;
            System.out.println(table.keySet().size());
        }while(file != null);
        writeOutputStepFileDouble(stepName,table);
    }

    private static void mergeFilesLong(File input,HashMap<String,Long> table){
        //read each line of file and update table by key and value.
        BufferedReader reader;
        try{
            reader = new BufferedReader(new FileReader(input));
            String line = reader.readLine();
            while(line != null){
                String[] arrLine = line.split("\\t");
                if(arrLine.length == 2){
                    long valueToAdd = Long.valueOf(arrLine[1]);
                    if(table.containsKey(arrLine[0])){
                        valueToAdd += (Long)table.get(arrLine[0]);
                    }
                    table.put(arrLine[0],valueToAdd);
                }
                else{
                    System.out.println("mergeFiles: " + Arrays.toString(arrLine));
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void mergeFilesDouble(File input,HashMap<String,Double> table){
        //read each line of file and update table by key and value.
        BufferedReader reader;
        try{
            reader = new BufferedReader(new FileReader(input));
            String line = reader.readLine();
            while(line != null){
                String[] arrLine = line.split("\\t");
                if(arrLine.length == 2){
                    double valueToAdd = Double.valueOf(arrLine[1]);
                    if(table.containsKey(arrLine[0])){
                        valueToAdd += (Double) table.get(arrLine[0]);
                    }
                    table.put(arrLine[0],valueToAdd);
                }
                else{
                    System.out.println("mergeFiles: " + Arrays.toString(arrLine));
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeOutputStepFileLong(String stepName,HashMap<String,Long> table){
        try {
            File file = new File(stepName + ".txt");
            file.createNewFile();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        // Write to the file
        try {
            FileWriter writer = new FileWriter(stepName + ".txt", true);
            PrintWriter out = new PrintWriter(writer);
            for (String key: table.keySet()){
                String toWrite = key + "\t" + table.get(key);
                out.append(toWrite +"\n");
            }
            out.close();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static void writeOutputStepFileDouble(String stepName,HashMap<String,Double> table){
        try {
            File file = new File(stepName + ".txt");
            file.createNewFile();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        // Write to the file
        try {
            FileWriter writer = new FileWriter(stepName + ".txt", true);
            PrintWriter out = new PrintWriter(writer);
            for (String key: table.keySet()){
                String toWrite = key + "\t" + table.get(key);
                out.append(toWrite +"\n");
            }
            out.close();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static String getKeyIndex(int count){
        String s1 = String.valueOf(count);
        int toAdd = 5-s1.length();
        for(int i=0 ;i<toAdd ; i++){
            s1 = "0" + s1;
        }
        return s1;
    }

    private static boolean waitForCluster(String clusterId) throws InterruptedException {
        //Valid Values: STARTING | BOOTSTRAPPING | RUNNING | WAITING | TERMINATING | TERMINATED | TERMINATED_WITH_ERRORS
        boolean stop = false;
        DescribeClusterResponse describeClusterResponse;
        ClusterState clusterStatus = null;

        while(!stop){
            describeClusterResponse = emr.describeCluster(DescribeClusterRequest.builder().clusterId(clusterId).build());
            clusterStatus = describeClusterResponse.cluster().status().state();
            if(clusterStatus.name().equals("TERMINATED") || clusterStatus.name().equals("TERMINATED_WITH_ERRORS"))
                stop = true;
            else
                TimeUnit.SECONDS.sleep(10);
        }
        if(clusterStatus!= null && clusterStatus.name().equals("TERMINATED")){
            return true;
        }
        return false;
    }

    private static File downloadTriGramProbabilityFile(){
        S3_Service.getInstance().makeKey("triGramProbability.txt/part-r-00000");
        return S3_Service.getInstance().downloadFromS3("triGramProbability.txt");
    }

    private static void uploadFile(String fileName) {
        try{
            S3_Service.getInstance().makeKey(fileName);
            S3_Service.getInstance().uploadInputToS3(fileName , fileName);
        }
        catch(Exception e){
            System.err.println("ERROR: can't upload file to server!\n");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void analysisOutputFile(ArrayList<AnalysisObject> arrAnalysisObjects) throws IOException {
        //sorted from lowest pdel to highest and if equal internal sort by number of instances
        try {
            File file = new File("analysisOutput.txt");
            file.createNewFile();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        // Write to the file
        try {
            FileWriter writer = new FileWriter("analysisOutput.txt");
            for (AnalysisObject a: arrAnalysisObjects)
                writer.write(a.toString()+ "\n");
            writer.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }

    private static ArrayList<AnalysisObject> analysis(File output){
        //prints 5 most frequent words of 10 words.
        ArrayList<AnalysisObject> arrAnalysisObjects = new ArrayList<>(); //sorted from lowest to highest
        int count = 0;
        int counter = 0;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(output));
            String line = reader.readLine();

            while (line != null) {
                System.out.println(counter + " " + line);
                counter++;
                //line is definitely unique
                //w1 w2 w3  pdel
                String[] lineArr = line.split("\\t");
                String trio = lineArr[0];
                double pdel = Double.parseDouble(lineArr[1]);
                String[] firstInstances = read4After(reader,trio);
                AnalysisObject analysisObject = new AnalysisObject(trio,firstInstances, Integer.parseInt(firstInstances[4]),pdel);
                if(count<10){
                    arrAnalysisObjects.add(analysisObject);
                    arrAnalysisObjects.sort(analysisObject.getComparator());
                    count++;
                }else{
                    analysisObject.addIfShould(arrAnalysisObjects); //sort is done there if needed.
                }
                line = reader.readLine();
            }
            reader.close();
            return arrAnalysisObjects;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String[] read4After(BufferedReader reader, String trio1) throws IOException {
        boolean stop = false;
        String[] toReturn = new String[5];
        int count = 0;
        int BUFFER_RESET = 1000000;
        reader.mark(BUFFER_RESET);
        String line = reader.readLine();
        while(!stop && line!=null){
            //read trio.
            String trio2 = line.split("\\t")[0];
            //add if they have the same 2 words and need to find more instance\s.
            if(checkIfEqual(trio1,trio2)){
                if(count < 4){
                    toReturn[count] = trio2;
                    count++;
                }
            }
            else{
                //new two w1w2 have been read. remember our file is sorted lexically.
                reader.reset();
                stop = true;
            }
            //if they have the same 2 words , but we don't need more instance , we just continue to "waste".
        }
        if(count == 0)
            return null;
        else
            toReturn[4] = String.valueOf(count);
        return toReturn;
    }

    private static boolean checkIfEqual(String trio1, String trio2){
        String[] arrTrio1 = trio1.split(" ");
        String[] arrTrio2 = trio1.split(" ");
        if(arrTrio1[0].equals(arrTrio2[0]))
            if(arrTrio1[1].equals(arrTrio2[1]))
                return true;
        return false;
    }

    private static void statistics(){
        String[] logs = {"syslog1.txt", "syslog2.txt", "syslog3.txt","syslog4.txt"};
        int i = 1;
        int sumKV = 0;
        int sumBytes = 0;
        String toWrite;

        for(String logName : logs){
            int[] answer = numberOfKeyValue(logName);
            toWrite = "Step " + i +":\n" + "Key-Value: " + answer[0] + "\n" + "Bytes: " + answer[1] + "\n\n";
            i++;
            writeStatistics(toWrite);
            sumKV += answer[0];
            sumBytes += answer[1];
        }

        toWrite = "Total:\n" + "Key-Value: " + sumKV + "\n" + "Bytes: " + sumBytes + "\n";

        writeStatistics(toWrite);

    }

    private static int[] numberOfKeyValue(String logName){

        int numOfKV = 0, kvSize = 0;
        String temp;
        int[] answer = new int[2]; // answer[0] = numOfKV, answer[1] = kvSize

        try {
            File file = new File(logName);
            Scanner reader = new Scanner(file);
            while (reader.hasNextLine()) {
                String line = reader.nextLine();

                if(line.startsWith("\t\tMap output records=")){
                    temp = line.substring("\t\tMap output records=".length());
                    numOfKV = Integer.parseInt(temp);
                }
                if(line.startsWith("\t\tMap output bytes=")){
                    temp = line.substring("\t\tMap output bytes=".length());
                    kvSize = Integer.parseInt(temp);
                }

            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        answer[0] = numOfKV;
        answer[1] = kvSize;

        return answer;
    }

    private static void writeStatistics(String toWrite){
        // Create a new file if necessary
        try {
            File file = new File("statistics.txt");
            file.createNewFile();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // Write to the file
        try {
            FileWriter writer = new FileWriter("statistics.txt", true);
            PrintWriter out = new PrintWriter(writer);
            out.append(toWrite);
            out.close();
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }



}

