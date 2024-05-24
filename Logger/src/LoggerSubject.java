import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoggerSubject {

    Map<Integer, List<LoggerObserver>> observerList = new HashMap<>();

    public void addObserver(int level, LoggerObserver observer)
    {
        List<LoggerObserver> obs = observerList.getOrDefault(level,new ArrayList<>());

        obs.add(observer);

        observerList.put(level,obs);
    }

    public void notifyAll(int level, String mssg)
    {
        for(Integer key: observerList.keySet())
        {
            if(key==level)
            {
                observerList.get(key).forEach(obs -> obs.log(mssg));
            }
        }
    }

}
