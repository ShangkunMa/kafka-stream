package kafka.example.stream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by MaShangkun on 18-9-5.
 */
public class LogHelper {
    public static Logger logger = LoggerFactory.getLogger("logHelper");

    public static void debug(String message) {
        logger.debug(message);
    }

    public static void info(String message) {
        logger.info(message);
    }

    public static void warn(String message) {
        logger.warn(message);
    }

    public static void error(String message) {
        logger.error(message);
    }

    public static void error(String message, Throwable throwable) {
        logger.error(message, throwable);
    }
}
