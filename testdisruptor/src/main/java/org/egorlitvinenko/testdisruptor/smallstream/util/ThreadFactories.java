package org.egorlitvinenko.testdisruptor.smallstream.util;

import java.util.concurrent.ThreadFactory;

/**
 * @author Egor Litvinenko
 */
public class ThreadFactories {

    public static ThreadFactory simpleDaemonFactory() {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        };
    }

}
