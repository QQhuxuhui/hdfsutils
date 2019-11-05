package log;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Auther: huxuhui
 * @Date: 2019/11/5 16:14
 * @Description:
 */
public class LogFileTest {

    @Test
    public void file(){
        Logger logger = LoggerFactory.getLogger(LogFileTest.class);
        logger.info("123");
    }
}
