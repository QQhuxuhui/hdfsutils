package hdfs.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Auther: huxuhui
 * @Date: 2019/10/9 10:48
 * @Description: HDFS小文件合并
 */
public class MergeSmallFile {

    private static Logger logger = LoggerFactory.getLogger(MergeSmallFile.class);

    private static String defaultFS = null;
    private static String folderPath = null;
    // 是否只合并.gz文件，默认否，只合并非.gz文件
    private static boolean gz = false;

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

    private ExecutorService es = Executors.newFixedThreadPool(2);
    private static long blockSize = 134217728;//128MB

    private Set<String> needMergerFolderPath = new HashSet<>();

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length > 3) {
            logger.error("parameters error!!!");
            logger.error("Parameter format:{} {} {}", "<fsUri>", "<path>", "<isGz>");
            logger.error("merge file parameters example: hdfs://xxx.xxx.xxx.xxx:8020 /xxx/xxx/xxx xx");
            return;
        }
//        if (args.length == 3) {
//            //自定义压缩块大小
//            blockSize = Long.parseLong(args[2]);
//        }
        MergeSmallFile mergeSmallFile = new MergeSmallFile();
        String[] uargs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        defaultFS = uargs[0];
        folderPath = uargs[1];
        if (uargs.length == 3) {
            gz = Boolean.parseBoolean(uargs[2]);
        }
        logger.info("fsUri:{},path:{}", defaultFS, folderPath);
        mergeSmallFile.merge();
    }


    public void merge() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(defaultFS), configuration);
        getNeedMergeFolderPathList(fileSystem, new Path(folderPath));
        logger.info("need merge file num:{}", needMergerFolderPath.size());
        if (needMergerFolderPath.size() == 0) {
            return;
        }
        //输入流、输出流
        for (String path : needMergerFolderPath) {
            es.execute(new MergeThread(path, fileSystem));
        }
        es.shutdown();
        while (!es.isTerminated()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("merge job complete, exit...");
        logger.info("exit success, bye bye!");
    }

    private void getNeedMergeFolderPathList(FileSystem fileSystem, Path path) throws IOException {
        //判断是否为文件夹
        for (FileStatus fileStatus : fileSystem.globStatus(path)) {
            if (fileStatus.isFile()) {
                needMergerFolderPath.add(fileStatus.getPath().getParent().toUri().toString());
                if (needMergerFolderPath.size() % 100 == 0) {
                    logger.info("need merge folder num:{}", needMergerFolderPath.size());
                }
            }
            if (fileStatus.isDirectory()) {
                getNeedMergeFolderPathList(fileSystem, new Path(fileStatus.getPath().toUri().getPath().concat("/*")));
            }
        }
    }

    /**
     * 合并线程，参数为接收一个目录，合并目录下的小文件
     */
    class MergeThread implements Runnable {

        String folderPath;
        FileSystem fileSystem;

        //存放老文件的目录列表
        Set<FileStatus> oldFiles = new HashSet<>();
        //存放新文件的目录列表
        Set<String> newFiles = new HashSet<>();

        public MergeThread(String folderPath, FileSystem fileSystem) {
            this.folderPath = folderPath;
            this.fileSystem = fileSystem;
        }

        /**
         * 1、获取目录下需要合并的文件列表
         * 2、遍历合并列表合并碎文件数据至新生成的文件内，不断检查新文件大小，如果超过定义大小再新生成一个文件继续合并，
         * 并将已合并的碎文件记录至oldFiles集合，新生成的文件记录至newFiles集合
         * 3、合并完毕删除oldFileList集合内记录的文件
         * 4、发生异常删除新生成的文件
         */
        @Override
        public void run() {
            //获取所有文件列表
            FileStatus[] fileStatuses = new FileStatus[0];
            try {
                fileStatuses = fileSystem.globStatus(new Path(folderPath + "/*"));
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            int fileCount = fileStatuses.length;
            if (fileCount <= 1) {
                return;
            }
            for (int index = 0; index <= fileCount - 1; index++) {
                //忽略相对的大文件
                if (fileStatuses[index].getLen() >= blockSize / 2) {
                    continue;
                }
                //非文件过滤
                if (!fileStatuses[index].isFile()) {
                    continue;
                }
                //过滤.tmp临时文件
                if (fileStatuses[index].getPath().getName().endsWith(".tmp")) {
                    continue;
                }
                //指定压缩文件模式下，过滤非压缩文件
                if (gz && !fileStatuses[index].getPath().getName().contains(".gz")) {
                    continue;
                }
                //指定非压缩文件模式下，过滤压缩文件
                if (!gz && fileStatuses[index].getPath().getName().contains(".gz")) {
                    continue;
                }
                oldFiles.add(fileStatuses[index]);
            }
            //判定需要压缩的文件数量如果0或者1，则不需要再压缩
            if (oldFiles.size() <= 1) {
                return;
            }
            /**
             * 2、开始遍历压缩
             */
            logger.info("{},need merge file count:{}", folderPath, oldFiles.size());
            long filesSize = 0;//统计已经合并的文件大小
            String outFileName;
            //数据合并后的文件
            Path outFilePath = null;
            //OutputStream to write to
            FSDataOutputStream out = null;
            try {
                int index = 0;//统计数据
                for (FileStatus oldFile : oldFiles) {
                    index++;
                    //创建合并文件
                    if (outFilePath == null) {
                        //如果大小为0，创建新文件用来准备合并
                        outFileName = oldFile.getPath().getName().split("\\.")[0] + "." + System.currentTimeMillis();
                        if (oldFile.getPath().getName().contains(".gz") && gz) {
                            outFileName = outFileName + ".gz";
                        }
                        outFilePath = new Path(folderPath + "/" + outFileName);
                        out = fileSystem.create(outFilePath);
                        newFiles.add(folderPath + "/" + outFileName);//加入新文件列表
                    }
                    //OutputStream to write from
                    FSDataInputStream in = fileSystem.open(oldFile.getPath());
                    //合并
                    IOUtils.copyBytes(in, out, configuration, false);
                    /**
                     * 1、关闭合并完的数据流
                     * 2、累计合并的大小，判定是否需要重新创建文件
                     */
                    IOUtils.closeStream(in);
                    filesSize = filesSize + oldFile.getLen();
                    if (filesSize >= blockSize) {
                        filesSize = 0;
                        outFilePath = null;
                    }
                    if (index % 10 == 0 || index == fileCount - 1) {
                        logger.info("foler:{},total:{},current:{},size:{}KB,{}MB", folderPath, fileCount, index + 1, filesSize / 1024, filesSize / (1024 * 1024));
                    }
                }
                //合并完毕，关闭数据流，删除旧文件
                IOUtils.closeStream(out);
                int delCount = 0;
                for (FileStatus oldFile : oldFiles) {
                    delCount++;
                    try {
                        fileSystem.delete(oldFile.getPath(), true);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    if (delCount % 10 == 0 || delCount == oldFiles.size()) {
                        logger.info("delete old file,filePath:{}, current:{}, total:{}", folderPath, delCount, oldFiles.size());
                    }
                }
            } catch (Exception e) {
                //合并异常，回滚数据文件
                e.printStackTrace();
                logger.info("merge fail start rollback...");
                for (String newFile : newFiles) {
                    try {
                        fileSystem.delete(new Path(newFile), true);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
                logger.info("rollback success...");
            } finally {
                IOUtils.closeStream(out);
            }
        }
    }

}
