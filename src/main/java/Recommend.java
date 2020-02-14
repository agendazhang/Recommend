
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//
//import HDFSAPI;

public class Recommend {
    public static final String HDFS = "/Recommend";

    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();
        //path for local data
        path.put("data", "/Recommend/data.csv");

        //step1 i/o path
        path.put("Step1Input", HDFS);
        path.put("Step1Output", path.get("Step1Input") + "/step1");

        //step2 i/o path
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");

        //step3 i/o path
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output", path.get("Step1Input") + "/step3");

        //step4 i/o path
        path.put("Step4Input", path.get("Step3Output"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        /*
        path.put("Step4_1Input1", path.get("Step3Output1"));
        path.put("Step4_1Input2", path.get("Step3Output2"));
        path.put("Step4_1Output", path.get("Step1Input") + "/step4_1");
        path.put("Step4_2Input", path.get("Step4_1Output"));
        path.put("Step4_2Output", path.get("Step1Input") + "/step4_2");
        */

        //step5 i/o path
        path.put("Step5Input", path.get("Step4Output"));
        path.put("Step5Output", path.get("Step1Input") + "/step5");


        Step1.run(path);
        Step2.run(path);
        Step3.run(path);
        Step4.run(path);
        Step5.run(path);

        //example to show result
        /*
        HDFSAPI hdfs = new HDFSAPI(new Path(HDFS));
        System.out.println(path.get("Step5Output")+"/part-r-00000");
        hdfs.readFile(new Path(path.get("Step5Output")+"/part-r-00000"));
        */

        System.exit(0);

    }
    public static Configuration config() {
        Configuration conf = new Configuration();
        return conf;
    }
}
