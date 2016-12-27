package logger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CLILog {
    private static Logger logger = null;
    
    public static Logger getInstance() {
        if (logger == null) {
            logger = LogManager.getLogger(CLILog.class.getName());
        }
        return logger;
    }
    
}
