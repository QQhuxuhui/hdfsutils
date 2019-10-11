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


### 1. 解压缩文件(Gzip格式)
```
java -cp HDFSUtils-1.0-jar-with-dependencies.jar hdfs.compress.CompressFile <fsUri> <path> <type> <isAll>
```

##### 参数说明
- <fsUri>：hdfs地址，例如hdfs://xxx.xxx.xxx.xxx:8020
- <path>：需要合并的目录路径，例如解压缩hdfs上面位于/data/a目录下的文件，参数就为/data/a或者/data/a/或者/data/a/*都可以
- <type>:
    - compress:压缩
    - uncompress:解压
- <isAll>，缺省值为false:
    - false：只解/压缩当前目录下的文件，不包括目录下的子目录   
    - true：解/压缩当前目录以及当前目录子目录下的所有文件
### 2. 合并指定目录下的小文件
```
java -cp HDFSUtils-1.0-jar-with-dependencies.jar hdfs.merge.MergeSmallFile <fsUri> <path>
```
##### 参数说明
- <fsUri>：hdfs地址，例如hdfs://xxx.xxx.xxx.xxx:8020
- <path>：需要合并的目录路径，例如需要合并hdfs上面位于/data/a目录下的所有小文件，/data/a或者/data/a/或者/data/a/*都可以
