package hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Auther: huxuhui
 * @Date: 2019/10/9 10:58
 * @Description: 压缩文件
 */
public class CompressFile {

    private static Logger logger = LoggerFactory.getLogger(CompressFile.class);

    private static String defaultFS = null;
    private static Configuration configuration = new Configuration();

    static {
        configuration.set("dfs.support.append", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        /**
         * 这个配置主要是针对打包的时候的问题：
         * hadoop filesystem相关的包有两个，分别是：hadoop-hdfs-2.7.1.jar和hadoop-common-2.7.1.jar
         * 指定即可，否则会出现java.io.IOException: No FileSystem for scheme: hdfs的异常
         */
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    private static ExecutorService es;

    /**
     * 是否递归操作目录以及子目录下的所有文件
     * true：递归遍历所有文件
     * false：只操作当前目录下的文件
     */
    private static boolean isAll = false;

    /**
     * 存储文件列表
     */
    private Set<String> filePathSet = new LinkedHashSet<>();

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args.length > 5) {
            logger.error("parameters error!!!");
            logger.error("Parameter format:{} {} {} {} {}", "<fsUri>", "<path>", "<type>", "<threads>", "<isAll>");
            logger.error("compress file example: hdfs://xxx.xxx.xxx.xxx:8020 /xxx/xxx/xxx/ compress true");
            logger.error("uncompress file example: hdfs://xxx.xxx.xxx.xxx:8020 /xxx/xxx/xxx/ uncompress true");
            return;
        }
        if (args.length == 5) {
            isAll = Boolean.parseBoolean(args[4]);
        }
        int threadNum = Integer.parseInt(args[3]);
        es = Executors.newFixedThreadPool(threadNum);
        CompressFile compressFile = new CompressFile();
        String[] uargs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        defaultFS = uargs[0];
        String sourceDir = uargs[1];
        if (!sourceDir.endsWith("*")) {
            sourceDir = sourceDir.concat("/*");
        }
        String type = uargs[2];
        if ("compress".equals(type)) {
            compressFile.compress(sourceDir);
        } else if ("uncompress".equals(type)) {
            compressFile.uncompress(sourceDir);
        } else {
            logger.error("params error");
        }

        while (!es.isTerminated()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if ("compress".equals(type)) {
            logger.info("compress job complete, exit...");
        }else{
            logger.info("uncompress job complete, exit...");
        }
        logger.info("exit success, bye bye!");
    }

    private void compress(String sourceDir) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(defaultFS), configuration);
        //递归获取所有需要压缩的文件列表
        getNeedCompressFilePathList(fileSystem, new Path(sourceDir));
        logger.info("total file num:{}", filePathSet.size());
        for (String path : filePathSet) {
            es.execute(new GzipCompressThread(defaultFS, path, configuration, fileSystem));
        }
        es.shutdown();
        filePathSet.clear();
    }


    private void uncompress(String sourceDir) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(defaultFS), configuration);
        //递归获取所有需要压缩的文件列表
        getNeedUnCompressFilePathList(fileSystem, new Path(sourceDir));
        logger.info("total file num:{}", filePathSet.size());
        for (String path : filePathSet) {
            es.execute(new GzipUncompressThread(defaultFS, path, configuration, fileSystem));
        }
        es.shutdown();
        filePathSet.clear();
    }


    /**
     * 递归遍历获取目录下的所有需要压缩的文件（非文件夹）列表
     * 这里的判断文件格式比较简单，直接用是否含有.gz判断！
     *
     * @param fileSystem
     * @param path
     * @throws IOException
     */
    private void getNeedCompressFilePathList(FileSystem fileSystem, Path path) throws IOException {
        //判断是否为文件夹
        for (FileStatus fileStatus : fileSystem.globStatus(path)) {
            if (fileStatus.isFile() && !fileStatus.getPath().getName().contains(".gz")) {
                filePathSet.add(fileStatus.getPath().toUri().toString());
                logger.info("need compress file num:{}", filePathSet.size());
            }
            if (fileStatus.isDirectory() && isAll) {
                getNeedCompressFilePathList(fileSystem, new Path(fileStatus.getPath().toUri().getPath().concat("/*")));
            }
        }
    }


    /**
     * 递归遍历获取目录下的所有需要解压的文件（非文件夹）列表
     *
     * @param fileSystem
     * @param path
     * @throws IOException
     */
    private void getNeedUnCompressFilePathList(FileSystem fileSystem, Path path) throws IOException {
        //判断是否为文件夹
        for (FileStatus fileStatus : fileSystem.globStatus(path)) {
            if (fileStatus.isFile() && fileStatus.getPath().getName().contains(".gz")) {
                filePathSet.add(fileStatus.getPath().toUri().toString());
            }
            if (fileStatus.isDirectory() && isAll) {
                getNeedUnCompressFilePathList(fileSystem, new Path(path.toUri().getPath().concat("/*")));
            }
        }
    }
}
