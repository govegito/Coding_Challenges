public abstract class AbstractLogger {

    private int level;
    private AbstractLogger nextLogger;

    protected AbstractLogger(int level, AbstractLogger nextLog)
    {
        this.level=level;
        this.nextLogger=nextLog;
    }

    protected void log(int level, String mssg, LoggerSubject subject){

        if(this.level==level)
        {
            display(level,mssg,subject);
        }

        if(nextLogger!=null)
        {
            nextLogger.log(level,mssg,subject);
        }
    }

    protected void display(int level, String mssg, LoggerSubject subject){

        subject.notifyAll(level,mssg);
    };

}
