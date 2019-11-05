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

    private ExecutorService es = Executors.newFixedThreadPool(3);
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
    }

    private void getNeedMergeFolderPathList(FileSystem fileSystem, Path path) throws IOException {
        //判断是否为文件夹
        for (FileStatus fileStatus : fileSystem.globStatus(path)) {
            if (fileStatus.isFile()) {
                needMergerFolderPath.add(fileStatus.getPath().getParent().toUri().toString());
                if (needMergerFolderPath.size() % 100 == 0) {
                    logger.info("need merge file num:{}", needMergerFolderPath.size());
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

        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        //存放老文件的目录列表
        Set<String> oldFiles = new HashSet<>();
        //存放新文件的目录列表
        Set<String> newFiles = new HashSet<>();

        public MergeThread(String folderPath, FileSystem fileSystem) {
            this.folderPath = folderPath;
            this.fileSystem = fileSystem;
        }

        @Override
        public void run() {
            try {
                FileStatus[] fileStatuses = fileSystem.globStatus(new Path(folderPath + "/*"));
                int fileCount = fileStatuses.length;
                if (fileCount <= 0) {
                    return;
                }
                if (fileCount == 1) {
                    return;
                }
                //遍历文件，计算小文件大小，忽略大文件
                //记录需要导入的文件名称
                String outFileName = null;
                Path outFilePath = null;
                long filesSize = 0;//一定数量的小文件的总长度
                //统计
                for (int index = 0; index <= fileCount - 1; index++) {
                    if (filesSize == 0) {
                        if (!fileStatuses[index].isFile()) {
                            //非文件过滤
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
                        //.tmp文件不作处理
                        if (fileStatuses[index].getPath().getName().endsWith(".tmp")) {
                            continue;
                        }
                        //重新计数，生成文件名
                        if (outFileName == null) {
                            outFileName = fileStatuses[index].getPath().getName().split("\\.")[0] + "." + System.currentTimeMillis();
                            if (fileStatuses[index].getPath().getName().contains(".gz") && gz) {
                                outFileName = outFileName + ".gz";
                            }
                            outFilePath = new Path(folderPath + "/" + outFileName);
                            newFiles.add(outFilePath.toUri().getPath());
                        }
                        if (out != null) {
                            out.close();
                        }
                        if (fileSystem.exists(outFilePath)) {
                            out = fileSystem.append(outFilePath);
                        } else {
                            out = fileSystem.create(outFilePath);
                        }
                    }
                    //忽略相对的大文件
                    if (fileStatuses[index].getLen() >= blockSize) {
                        continue;
                    }
                    filesSize += fileStatuses[index].getLen();
                    if (filesSize >= blockSize) {
                        //重新开始计数
                        filesSize = 0;
                        index--;//index回退
                        outFileName = null;
                    } else {
                        //写入数据
                        if (!fileStatuses[index].getPath().getName().equals(outFilePath.getName())) {
                            //不是同一个文件，写入
                            in = fileSystem.open(fileStatuses[index].getPath());
                            IOUtils.copyBytes(in, out, 10485760, false);
//                            实时写入
                            out.hsync();
                            oldFiles.add(fileStatuses[index].getPath().toUri().getPath());
                        }
                    }
                    if (in != null) {
                        in.close();
                    }
                    if (index % 10 == 0 || index == fileCount - 1) {
                        logger.info("foler:{},total:{},current:{},size:{}KB", folderPath, fileCount, index + 1, filesSize / 1024);
                    }
                }
                //合并完毕，删除旧文件
                int delCount = 0;
                for (String oldFile : oldFiles) {
                    delCount++;
                    try {
                        fileSystem.delete(new Path(oldFile), true);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    if (delCount % 10 == 0 || delCount == oldFiles.size()) {
                        logger.info("delete old file, current:{}, total:{}", delCount, oldFiles.size());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.info("merge fail start rollback...");
                for (String newFile : newFiles) {
                    try {
                        fileSystem.delete(new Path(newFile), true);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
                logger.info("rollback complete...");
            }
        }
    }

}
