public class Logger {

    private static volatile Logger logger;
    private static volatile  AbstractLogger loggerChain;

    private static volatile LoggerSubject loggerSubject;

    private Logger() {
    }

    public static Logger getLogger() {
        if (logger == null) {
            synchronized (Logger.class) {
                if (logger == null) {
                    logger = new Logger();
                    loggerChain = LogManager.getLoggerChain();
                    loggerSubject=LogManager.getLogSubject();

                }
            }
        }

        return logger;
    }

    public void info(String mssg) {
        loggerChain.log(1, mssg,loggerSubject);
    }

    public void error(String mssg) {
        loggerChain.log(2, mssg,loggerSubject);
    }

    public void debug(String mssg) {
        loggerChain.log(3, mssg,loggerSubject);
    }
}