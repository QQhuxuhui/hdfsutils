package hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * @Auther: huxuhui
 * @Date: 2019/10/11 09:46
 * @Description:
 */
public class GzipUncompressThread implements Runnable {

    private Logger logger = LoggerFactory.getLogger(GzipUncompressThread.class);

    private final static String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";

    private String defaultFS;

    //需要压缩的文件
    private String sourceFile;

    //配置
    private Configuration configuration;

    private FileSystem fileSystem;

    public GzipUncompressThread(String defaultFS, String path, Configuration configuration, FileSystem fileSystem) {
        this.sourceFile = path;
        this.defaultFS = defaultFS;
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }

    @Override
    public void run() {
        if (!sourceFile.contains(".gz")) {
            logger.error("file:{} is not gz file.", sourceFile);
            return;
        }
        String goal_dir = sourceFile.replace(".gz", "");
        logger.info("start uncompress file:{},target file:{}", sourceFile, goal_dir);
        //压缩文件
        Class<?> codecClass = null;
        FSDataInputStream input = null;
        OutputStream output = null;
        try {
            codecClass = Class.forName(codecClassName);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
            //指定压缩文件输出路径
            input = fileSystem.open(new Path(sourceFile));
            CompressionInputStream codec_input = codec.createInputStream(input);
            output = fileSystem.create(new Path(goal_dir));
            IOUtils.copyBytes(codec_input, output, configuration);
            fileSystem.delete(new Path(sourceFile), true);
            logger.info("compress success delete:{}", sourceFile);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("compress file fail,roll back....");
            if (fileSystem != null) {
                try {
                    fileSystem.delete(new Path(goal_dir), true);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
                IOUtils.closeStream(input);
                IOUtils.closeStream(output);
        }
    }
}
