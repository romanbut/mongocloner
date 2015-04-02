package com.publicreputation.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ExtendedQueue<E> extends ConcurrentLinkedQueue<E> {
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicLong readCount = new AtomicLong(0l);
    private final AtomicLong writeCount = new AtomicLong(0l);
    private final AtomicLong totalCount = new AtomicLong(0l);


    public boolean processing() {
        return !isEmpty() || !finished.get();
    }

    public void shutdown() {
        finished.set(true);
    }

    public long getReadCount() {
        return readCount.get();
    }

    public long getTotal() {
        return totalCount.get();
    }

    public long getWriteCount() {
        return writeCount.get();
    }

    public void incrementReadCount() {
        readCount.incrementAndGet();
    }

    public void incrementWriteCount() {
        writeCount.incrementAndGet();
    }

    public void setTotalCount(final long total) {
        totalCount.set(total);
    }

}
