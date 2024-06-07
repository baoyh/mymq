package bao.study.mymq.remoting;

/**
 * @author baoyh
 * @since 2022/5/13 15:20
 */
public interface Remoting {

    void start();

    void shutdown();

    boolean hasStarted();
}
