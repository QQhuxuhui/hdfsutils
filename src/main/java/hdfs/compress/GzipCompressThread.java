package hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * @Auther: huxuhui
 * @Date: 2019/10/9 11:23
 * @Description: Gzip压缩，压缩结果文件以.gz结尾，线程内根据传入的文件路径压缩
 */
public class GzipCompressThread implements Runnable {

    private Logger logger = LoggerFactory.getLogger(GzipCompressThread.class);

    private final static String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";

    private String defaultFS;

    //需要压缩的文件
    private String sourceFile;

    //配置
    private Configuration configuration;

    private FileSystem fileSystem;


    public GzipCompressThread(String defaultFS, String sourceFile, Configuration configuration, FileSystem fileSystem) {
        this.sourceFile = sourceFile;
        this.defaultFS = defaultFS;
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }

    @Override
    public void run() {
        String gzipFileDir = sourceFile.concat(".gz");
        logger.info("start compress file:{},target file:{}", sourceFile, gzipFileDir);
        //压缩文件
        Class<?> codecClass = null;
        FSDataInputStream in = null;
        CompressionOutputStream out = null;
        try {
            codecClass = Class.forName(codecClassName);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);

            //指定压缩文件输出路径
            FSDataOutputStream outputStream = fileSystem.create(new Path(gzipFileDir));
            //指定被压缩的文件路径
            in = fileSystem.open(new Path(sourceFile));
            //创建压缩输出流
            out = codec.createOutputStream(outputStream);
            IOUtils.copyBytes(in, out, configuration);
            fileSystem.delete(new Path(sourceFile), true);
            logger.info("compress success delete:{}", sourceFile);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("compress file fail,roll back....");
            try {
                if (fileSystem != null) {
                    fileSystem.delete(new Path(gzipFileDir), true);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
