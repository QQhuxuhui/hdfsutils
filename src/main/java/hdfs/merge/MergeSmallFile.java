package hdfs.merge;

import hdfs.compress.CompressFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
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

    private ExecutorService es = Executors.newFixedThreadPool(5);
    private static long blockSize = 134217728;//128MB

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length > 3) {
            logger.error("parameters error!!!");
            logger.error("Parameter format:{} {}", "<fsUri>", "<path>");
            logger.error("merge file parameters example: hdfs://xxx.xxx.xxx.xxx:8020 /xxx/xxx/xxx");
            return;
        }
        if (args.length == 3) {
            //自定义压缩块大小
            blockSize = Long.parseLong(args[2]);
        }
        MergeSmallFile mergeSmallFile = new MergeSmallFile();
        String[] uargs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        defaultFS = uargs[0];
        folderPath = uargs[1];
        logger.info("fsUri:{},path:{}", defaultFS, folderPath);
        mergeSmallFile.merge();
    }


    public void merge() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(defaultFS), configuration);
        FileStatus[] fileStatusArr = fileSystem.globStatus(new Path(folderPath));
        //输入流、输出流
        for (FileStatus fileStatus : fileStatusArr) {
            es.execute(new MergeThread(fileStatus, fileSystem));
        }
        es.shutdown();
    }


    class MergeThread implements Runnable {

        FileStatus fileStatus;
        FileSystem fileSystem;

        FSDataOutputStream out = null;
        FSDataInputStream in = null;

        public MergeThread(FileStatus fileStatus, FileSystem fileSystem) {
            this.fileStatus = fileStatus;
            this.fileSystem = fileSystem;
        }

        @Override
        public void run() {
            try {
                FileStatus[] fileStatuses = fileSystem.globStatus(new Path(fileStatus.getPath() + "/*"));
                int fileCount = fileStatuses.length;
                if (fileCount == 0) {
//                    fileSystem.delete(fileStatus.getPath(), true);
                    return;
                }
                if (fileCount == 1) {
                    return;
                }
                //遍历文件，计算小文件大小，忽略文件大小大于60MB的文件
                //记录需要导入的文件名称
                String outFileName = null;
                Path outFilePath = null;
                long filesSize = 0;//一定数量的小文件的总长度
                for (int index = 0; index <= fileCount - 1; index++) {
                    if (filesSize == 0) {
                        if (!fileStatuses[index].isFile()) {
                            //非文件夹过滤
                            continue;
                        }
                        //重新计数，生成文件名
                        if (outFileName == null) {
                            if (fileStatuses[index].getPath().getName().endsWith("_") && fileStatuses[index].getLen() < blockSize) {
                                outFileName = fileStatuses[index].getPath().getName();
                                outFilePath = new Path(fileStatus.getPath() + "/" + outFileName);
                            } else {
                                outFileName = fileStatuses[index].getPath().getName() + "_";
                                outFilePath = new Path(fileStatus.getPath() + "/" + outFileName);
                            }
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
                    if (fileStatuses[index].getLen() >= blockSize / 2) {
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
                            IOUtils.copyBytes(in, out, 4096, false);
                            //实时写入
                            out.hsync();
                            fileSystem.delete(fileStatuses[index].getPath(), true);
                        }
                    }
                    if (in != null) {
                        in.close();
                    }
                    logger.info("file:{},total:{},current:{},size:{}KB", fileStatus.getPath().getName(), fileCount, index + 1, filesSize / 1024);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
