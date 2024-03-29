# HDFSUtils

> utils for hdfs

## 介绍
操作hdfs文件的工具集合，包括文件解压缩、碎文件合并等

### 运行环境
- jdk1.7+
- hdfs2.x

### 编译打包

```
mvn clean 
mvn assembly:assembly
```

## 使用说明


### 1. 批量解压缩文件(Gzip格式)
```
java -cp HDFSUtils-1.0-jar-with-dependencies.jar hdfs.compress.CompressFile <fsUri> <path> <type> <threads> <isAll>

或者使用hadoop命令运行
hadoop jar HDFSUtils-1.0-jar-with-dependencies.jar hdfs.compress.CompressFile <fsUri> <path> <type> <threads> <isAll>
```
**<fsUri> <path>之间有空格，要特别注意！！！！**
##### 参数说明
- \<fsUri\>：hdfs地址，例如hdfs://xxx.xxx.xxx.xxx:8020，如果使用hadoop命令，这里的地址可以使用namenode的servicename
- \<path\>：需要合并的目录路径，例如解压缩hdfs上面位于/data/a目录下的文件，参数就为/data/a或者/data/a/或者/data/a/*都可以
- \<type\>:
    - compress:压缩
    - uncompress:解压
- \<threads\>：压缩文件的并发度，压缩文件占用服务器带宽资源较为明显，可以适当调小并发度。 
- \<isAll\>，缺省值为false:
    - false：只解/压缩当前目录下的文件，不包括目录下的子目录   
    - true：解/压缩当前目录以及当前目录子目录下的所有文件

**再强调一遍，<fsUri> <path>之间有空格，要特别注意！！！！**
### 2. 合并指定目录下的小文件
```
java -cp HDFSUtils-1.0-jar-with-dependencies.jar hdfs.merge.MergeSmallFile <fsUri> <path> <isGz>

或者使用hadoop命令运行
hadoop jar HDFSUtils-1.0-jar-with-dependencies.jar hdfs.merge.MergeSmallFile <fsUri> <path> <isGz>
```
##### 参数说明
- \<fsUri\>：hdfs地址，例如hdfs://xxx.xxx.xxx.xxx:8020，如果使用hadoop命令，这里的地址可以使用namenode的servicename
- \<path\>：需要合并的目录路径，会遍历目录下的所有子目录。
- \<isGz\>：是否合并压缩过的.gz文件，注意，这里以文件名.gz来区分是否是压缩过的文件
	- false：默认值，合并未经压缩的普通文件
	- true：只合并.gz的压缩过的文件 	
