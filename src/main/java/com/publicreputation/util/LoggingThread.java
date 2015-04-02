package com.publicreputation.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class LoggingThread<T> implements Runnable {
    private static final Long SLEEP_PERIOD = TimeUnit.SECONDS.toMillis(2);
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ExtendedQueue<T> queue;
    private final String collectionName;

    public LoggingThread(final ExtendedQueue<T> queue, final String collection) {
        this.queue = queue;
        this.collectionName = collection;
    }

    @Override
    public void run() {
        while (queue.processing()) {
            logger.info("Processed (READ/WRITE/TOTAL):  {}/{}/{} for `{}`", queue.getReadCount(), queue.getWriteCount(), queue.getTotal(), collectionName);
            try {
                Thread.sleep(SLEEP_PERIOD);
            } catch (InterruptedException e) {
                logger.warn("Logger thread error!", e);
            }
        }
        logger.info("Finished processing collection `{}`", collectionName);
    }
}
