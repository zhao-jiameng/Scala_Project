package twotest;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @PROJECT_NAME: BIgData
 * @PACKAGE_NAME: twotest
 * @author: 赵嘉盟-HONOR
 * @data: 2023-09-05 22:24
 * @DESCRIPTION
 */
public class flume implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }
}
