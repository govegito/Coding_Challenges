public class ConsoleLogger implements LoggerObserver{

    @Override
    public void log(String mssg) {

        System.out.println("Logging from the sink "+ mssg);
    }
}
