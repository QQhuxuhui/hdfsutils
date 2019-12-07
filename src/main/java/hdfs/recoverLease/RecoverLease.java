package hdfs.recoverLease;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: huxuhui
 * @Date: 2019/11/11 19:53
 * @Description: 释放HDFS文件租约
 */
public class RecoverLease {

    private static Logger logger = LoggerFactory.getLogger(RecoverLease.class);

    public static void main(String[] args) throws IOException {
        RecoverLease recoverLease = new RecoverLease();


        List<String> lines = FileUtils.readLines(new File("C:\\Users\\GGbond\\Desktop\\dfs.out"));
        List<String> linesNew = new ArrayList<>();
        for (String line : lines) {
            String str = line.replace("..", "");
            if (StringUtils.isEmpty(str)) {
                continue;
            }
            for (String s : str.split(" ")) {
                if (s.contains("xrea")) {
                    if (s.startsWith("./xrea")) {
                        s = s.replace("./xrea/", "/xrea/");
                    }
                    linesNew.add(s);
                    System.out.println(s);
                }
            }
//            FileUtils.writeLines(new File("C:\\Users\\GGbond\\Desktop\\needReleaseFiles.txt"), linesNew);
        }
        int count = 0;
        for (String s : linesNew) {
            logger.info("total:{},current:{}", linesNew.size(), count++);
            recoverLease.recoverLease("hdfs://10.80.0.77:8020" + s);
        }
    }

    private void recoverLease(String path) {
        try {
            DistributedFileSystem fs = new DistributedFileSystem();
            Configuration conf = new Configuration();
            fs.initialize(URI.create(path), conf);
            fs.recoverLease(new Path(path));
            fs.close();
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }
}
