public class InfoLogger extends AbstractLogger{

    protected InfoLogger(int level, AbstractLogger nextLog) {
        super(level, nextLog);
    }

    @Override
    public void display(int level, String mssg, LoggerSubject subject)
    {
        String infoMssg="INFO: "+mssg;
        super.display(level,infoMssg,subject);
    }
}
