package org.apache.accumulo.master.status;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Runs one or more tasks with a timeout per task (instead of for all tasks).
 *
 * @param <T> The return type of the submitted Callables.
 * @param <C> The type of the Callables.
 */
public class TimeoutTaskExecutor<T, C extends Callable<T>> implements AutoCloseable {

    private final static Logger log = LoggerFactory.getLogger(TimeoutTaskExecutor.class);

    private final long timeout;
    private final ExecutorService executorService;
    private final List<WrappedTask> wrappedTasks;

    private BiConsumer<C, T> successCallback;
    private BiConsumer<C, Exception> exceptionCallback;
    private Consumer<C> timeoutCallback;

    public TimeoutTaskExecutor(ExecutorService executorService, long timeout, int expectedNumCallables) {
        this.executorService = executorService;
        this.timeout = timeout;
        this.wrappedTasks = Lists.newArrayListWithExpectedSize(expectedNumCallables);
    }

    /**
     * Submits a new task to the underlying executor.
     *
     * @param callable
     */
    public void submit(C callable) {
        WrappedTask wt = new WrappedTask(callable, executorService.submit(callable));
        wrappedTasks.add(wt);
    }

    /**
     * Registers the callback to use on successful tasks.
     *
     * @param callback
     */
    public void onSuccess(BiConsumer<C, T> callback) {
        this.successCallback = callback;
    }

    /**
     * Registers the callback to use on tasks that generating an exception.
     *
     * @param callback
     */
    public void onException(BiConsumer<C, Exception> callback) {
        this.exceptionCallback = callback;
    }

    /**
     * Registers the callback to use on tasks that timed out.
     *
     * @param callback
     */
    public void onTimeout(Consumer<C> callback) {
        this.timeoutCallback = callback;
    }

    /**
     * Completes all the current tasks by dispatching to tha appropriate callback.
     *
     * @throws InterruptedException If interrupted while awaiting callable results.
     */
    public void complete() throws InterruptedException {
        Preconditions.checkState(successCallback != null, "Must set a success callback before completing " + this);
        Preconditions.checkState(exceptionCallback != null, "Must set an exception callback before completing " + this);
        Preconditions.checkState(timeoutCallback != null, "Must set a timeout callback before completing " + this);

        for (WrappedTask wt : wrappedTasks) {
            try {
                successCallback.accept(wt.callable, wt.future.get(timeout, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                throw e;
            } catch (ExecutionException e) {
                exceptionCallback.accept(wt.callable, e);
            } catch (TimeoutException e) {
                wt.future.cancel(true);
                timeoutCallback.accept(wt.callable);
            }
        }
    }

    @Override
    public void close() {
        try {
            executorService.shutdownNow();
        } catch (Exception e) {
            log.warn("Error while shutting down " + this, e);
        }
    }

    /*
     * A wrapper for keeping a callable together with it's corresponding future and results.
     */
    private class WrappedTask {
        public C callable;
        public Future<T> future;

        public WrappedTask(C callable, Future<T> future) {
            this.callable = callable;
            this.future = future;
        }
    }
}
