package org.mosence.tinkerpop.gremlin.mongodb.process.util;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

public final class ComputerSubmissionHelper {

    /**
     * Creates a {@link Executors#newSingleThreadExecutor(ThreadFactory)} configured
     * make threads that behave like the caller, invokes a closure on it, and shuts it down.
     * <p>
     * This is intended to serve as an alternative to {@link ForkJoinPool#commonPool()},
     * which is used by {@link CompletableFuture#supplyAsync(Supplier)} (among other methods).
     * The single threaded executor created by this method contains a thread
     * with the same context classloader and thread group as the thread that called
     * this method.  Threads created in this method also have predictable behavior when
     * {@link Thread#setContextClassLoader(ClassLoader)} is invoked; threads in the
     * common pool throw a SecurityException if the JVM has a security manager configured.
     * <p>
     * The name of the thread created by this method's internal executor is the concatenation of
     * <ul>
     *     <li>the name of the thread that calls this method</li>
     *     <li>"-TP-"</li>
     *     <li>the {@code threadNameSuffix} parameter value</li>
     * </ul>
     *
     * @param closure arbitrary code that has exclusive use of the supplied executor
     * @param threadNameSuffix a string appended to the executor's thread's name
     * @return the return value of the {@code closure} parameter
     */
    public static Future<ComputerResult> runWithBackgroundThread(Function<Executor, Future<ComputerResult>> closure,
                                                                 String threadNameSuffix) {
        final Thread callingThread = Thread.currentThread();
        final ClassLoader classLoader = callingThread.getContextClassLoader();
        final ThreadGroup threadGroup = callingThread.getThreadGroup();
        final String threadName = callingThread.getName();
        ExecutorService submissionExecutor = null;

        try {
            submissionExecutor = Executors.newSingleThreadExecutor(runnable -> {
                final Thread t = new Thread(threadGroup, runnable, threadName + "-TP-" + threadNameSuffix);
                t.setContextClassLoader(classLoader);
                return t;
            });

            return closure.apply(submissionExecutor);
        } finally {
            if (null != submissionExecutor) {
                // do not call shutdownNow, which could prematurely terminate the closure
                submissionExecutor.shutdown();
            }
        }
    }
}
