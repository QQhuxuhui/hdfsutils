package hdfs.constants;

import java.nio.charset.Charset;

/**
 * @Auther: user
 * @Date: 2018/5/14 09:27
 * @Description:
 */
public interface Constants {
    Charset GLOBAL_CHARSET = Charset.forName("utf-8");
//    String CORE_XML_PATH = "/*/**/*.xml";
    String CORE_XML_PATH = "/config/*.xml";
}
