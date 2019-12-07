//package hdfs;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.lang.StringUtils;
//import org.junit.Test;
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class GgbondApplicationTests {
//
//    @Test
//    public void contextLoads() {
//        try {
//            List<String> lines = FileUtils.readLines(new File("C:\\Users\\GGbond\\Desktop\\dfs.out"));
//            List<String> linesNew = new ArrayList<>();
//            for (String line : lines) {
//                String str = line.replace("..", "");
//                if (StringUtils.isEmpty(str)) {
//                    continue;
//                }
//                for (String s : str.split(" ")) {
//                    if (s.contains("xrea")) {
//                        if(s.startsWith("./xrea")){
//                            s = s.replace("./xrea/", "/xrea/");
//                        }
//                        linesNew.add(s);
//                        System.out.println(s);
//                    }
//                }
//                FileUtils.writeLines(new File("C:\\Users\\GGbond\\Desktop\\needReleaseFiles.txt"), linesNew);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//}
