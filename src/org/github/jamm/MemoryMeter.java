package org.github.jamm;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;

public class MemoryMeter {
    private static Instrumentation inst;

    public static void premain(String options, Instrumentation inst) {
        MemoryMeter.inst = inst;
    }

    /**
     * IGNORE will completely ignore ByteBuffers that don't expose their entire capacity;
     * IGNORE_OVERHEAD (default) will include only the size that the buffer exposes (as if
     * by calling bb.remaining()): this will undercount if the buffers are fragmented; INCLUDE
     * will count each reference to a shared buffer with the entire retained capacity of the
     * buffer, which will overcount if there is more than one reference to the buffer.
     */
    public static class BufferBehavior
    {
        private BufferBehavior(){}

        /** @return The size of a shared buffer. */
        protected int sharedBufferSize(ByteBuffer buffer)
        {
            // reference [to array] + int offset
            return 8 + 4;
        }

        public static final BufferBehavior IGNORE = new BufferBehavior();

        public static final BufferBehavior IGNORE_OVERHEAD = new BufferBehavior()
        {
            protected int sharedBufferSize(ByteBuffer buffer)
            {
                // ... + bytes exposed by the buffer
                return super.sharedBufferSize(buffer) + buffer.remaining();
            }
        };

        public static final BufferBehavior INCLUDE = new BufferBehavior()
        {
            protected int sharedBufferSize(ByteBuffer buffer)
            {
                // ... + bytes retained by the buffer
                return super.sharedBufferSize(buffer) + buffer.capacity();
            }
        };
    }

    private final Callable<Set<Object>> trackerProvider;
    private final BufferBehavior bufferBehaviour;

    public MemoryMeter() {
        this(new Callable<Set<Object>>() {
            public Set<Object> call() throws Exception {
                // using a normal HashSet to track seen objects screws things up in two ways:
                // - it can undercount objects that are "equal"
                // - calling equals() can actually change object state (e.g. creating entrySet in HashMap)
                return Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
            }
        }, BufferBehavior.IGNORE_OVERHEAD);
    }

    /**
     * @param trackerProvider returns a Set with which to track seen objects and avoid cycles
     * @param bufferBehaviour
     */
    private MemoryMeter(Callable<Set<Object>> trackerProvider, BufferBehavior bufferBehaviour) {
        this.trackerProvider = trackerProvider;
        this.bufferBehaviour = bufferBehaviour;
    }

    /**
     * @param trackerProvider
     * @return a MemoryMeter with the given provider
     */
    public MemoryMeter withTrackerProvider(Callable<Set<Object>> trackerProvider) {
        return new MemoryMeter(trackerProvider, bufferBehaviour);
    }

    /**
     * @return a MemoryMeter that uses the given BufferBehavior to account for
     * shared buffers (where bb.remaining() != bb.capacity())
     */
    public MemoryMeter withBufferBehavior(BufferBehavior bufferBehaviour) {
        return new MemoryMeter(trackerProvider, bufferBehaviour);
    }

    /**
     * @return the shallow memory usage of @param object
     * @throws NullPointerException if object is null
     */
    public long measure(Object object) {
        if (inst == null) {
            throw new IllegalStateException("Instrumentation is not set; Jamm must be set as -javaagent");
        }
        return inst.getObjectSize(object);
    }

    /**
     * @return the memory usage of @param object including referenced objects
     * @throws NullPointerException if object is null
     */
    public long measureDeep(Object object) {
        if (object == null) {
            throw new NullPointerException(); // match getObjectSize behavior
        }

        Set<Object> tracker;
        try {
            tracker = trackerProvider.call();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        tracker.add(object);

        // track stack manually so we can handle deeper heirarchies than recursion
        Stack<Object> stack = new Stack<Object>();
        stack.push(object);

        long total = 0;
        while (!stack.isEmpty()) {
            Object current = stack.pop();
            assert current != null;
            total += measure(current);

            if (current instanceof Object[]) {
                addArrayChildren((Object[]) current, stack, tracker);
            } else if (current instanceof ByteBuffer) {
                total += measureBuffer(current);
            } else {
                addFieldChildren(current, stack, tracker);
            }
        }

        return total;
    }

    /**
     * @return the number of child objects referenced by @param object
     * @throws NullPointerException if object is null
     */
    public long countChildren(Object object) {
        if (object == null) {
            throw new NullPointerException();
        }

        Set<Object> tracker = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        tracker.add(object);
        Stack<Object> stack = new Stack<Object>();
        stack.push(object);

        long total = 0;
        while (!stack.isEmpty()) {
            Object current = stack.pop();
            assert current != null;
            total++;

            if (current instanceof Object[]) {
                addArrayChildren((Object[]) current, stack, tracker);
            }
            else {
                addFieldChildren(current, stack, tracker);
            }
        }

        return total;
    }

    public long measureBuffer(Object current)
    {
        ByteBuffer buffer = (ByteBuffer) current;
        if (buffer.remaining() != buffer.capacity())
            // buffer is shared
            return bufferBehaviour.sharedBufferSize(buffer);
        // unshared: reference [to array] + int offset + bytes in the buffer
        return 8 + 4 + buffer.remaining();
    }

    private void addFieldChildren(Object current, Stack<Object> stack, Set<Object> tracker) {
        Class cls = current.getClass();
        while (cls != null) {
            for (Field field : cls.getDeclaredFields()) {
                if (field.getType().isPrimitive() || Modifier.isStatic(field.getModifiers())) {
                    continue;
                }

                field.setAccessible(true);
                Object child;
                try {
                    child = field.get(current);
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                if (child != null && !tracker.contains(child)) {
                    stack.push(child);
                    tracker.add(child);
                }
            }

            cls = cls.getSuperclass();
        }
    }

    private void addArrayChildren(Object[] current, Stack<Object> stack, Set<Object> tracker) {
        for (Object child : current) {
            if (child != null && !tracker.contains(child)) {
                stack.push(child);
                tracker.add(child);
            }
        }
    }
}
