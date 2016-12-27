package logger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Log4j {
    private static Logger logger = null;
    
    public static Logger getInstance() {
        if (logger == null) {
            logger = LogManager.getLogger(Log4j.class.getName());
        }
        return logger;
    }
    
}
