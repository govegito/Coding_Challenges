public class LogManager {

    protected static AbstractLogger getLoggerChain()
    {
        AbstractLogger logger = new InfoLogger(1,new ErrorLogger(2,new DebugLogger(3,null)));

        return logger;
    }

    protected static LoggerSubject getLogSubject(){

        LoggerObserver console = new ConsoleLogger();

        LoggerSubject subject = new LoggerSubject();

        subject.addObserver(1,console);
        subject.addObserver(2,console);
        subject.addObserver(3,console);

        return subject;
    }
}
