package org.apache.accumulo.master.status;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TimeoutTaskExecutorTest {

    private TimeoutTaskExecutor<String, DummyTask> executor;
    private long timeout = 100;

    private Collection<String> results;
    private Collection<DummyTask> timeouts;

    @Before
    public void setup() {
        int numThreads = 2;
        executor = new TimeoutTaskExecutor<>(Executors.newFixedThreadPool(numThreads), timeout, 3);

        results = Lists.newArrayList();
        timeouts = Lists.newArrayList();

        executor.onSuccess((task, result) -> results.add(result));
        executor.onTimeout((task) -> timeouts.add(task));
        executor.onException((task, ex) -> fail("Unexpected exception"));
    }

    @Test
    public void shouldExecuteTasks() throws InterruptedException {
        executor.submit(new DummyTask("one", 0));
        executor.submit(new DummyTask("two", 0));

        executor.complete();

        assertThat(results.contains("one"), is(true));
        assertThat(results.contains("two"), is(true));
        assertThat(timeouts.isEmpty(), is(true));
    }

    @Test
    public void shouldReportTimedOutTasks() throws InterruptedException {
        executor.submit(new DummyTask("successful", 0));
        executor.submit(new DummyTask("timeout", timeout * 2));

        executor.complete();

        DummyTask task = Iterables.get(timeouts, 0);

        assertThat(timeouts.size(), is(1));
        assertThat(task.result, is("timeout"));
    }

    @Test
    public void slowTasksShouldNotPreventOthersFromRunning() throws Exception {
        // Clog up the threadpool with slow running tasks
        executor.submit(new DummyTask("slow task 1", Long.MAX_VALUE));
        executor.submit(new DummyTask("slow task 2", Long.MAX_VALUE));
        executor.submit(new DummyTask("slow task 3", Long.MAX_VALUE));
        executor.submit(new DummyTask("good task", 0L));

        executor.complete();

        assertThat(results.size(), is(1));
        assertThat(Iterables.getFirst(results, null), is("good task"));
    }

    @After
    public void tearDown() {
        executor.close();
    }

    private class DummyTask implements Callable<String> {
        private final String result;
        private final long timeout;

        public DummyTask(String result, long timeout) {
            this.result = result;
            this.timeout = timeout;
        }

        @Override
        public String call() throws Exception {
            Thread.sleep(timeout);
            return result;
        }
    }
}
