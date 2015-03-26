package com.publicreputation;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExtendedQueue<E> extends ConcurrentLinkedQueue<E> {
    private AtomicBoolean finished = new AtomicBoolean(false);

    public boolean processing() {
        return !isEmpty() || !finished.get();
    }

    public void shutdown() {
        finished.set(true);
    }

}
