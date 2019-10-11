package hdfs.bean;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: user
 * @Date: 2018/5/10 16:35
 * @Description: 保存配置文件的位置Bean
 */
public class XmlPathBean implements Serializable {

    //环境，读取的properties中的env，test为测试环境，product为生产环境
    String env;

    public List<String> xmlPathList = new ArrayList<>();

    public List<String> getXmlPathList() {
        return xmlPathList;
    }

    public void setXmlPathList(List<String> xmlPathList) {
        for(String xmlPath : xmlPathList){
            this.xmlPathList.add(MessageFormat.format(xmlPath, this.env));
        }
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }
}
