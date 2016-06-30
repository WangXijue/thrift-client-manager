package com.didi.thrift.pool;

import java.io.Closeable;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TServiceClient;

import com.didi.thrift.pool.exceptions.ThriftException;
import com.didi.thrift.pool.exceptions.ThriftPoolException;

public abstract class AbstractThriftPool<T extends TServiceClient> implements Closeable {
    protected GenericObjectPool<T> internalPool;

    /**
     * Using this constructor means you have to set and initialize the
     * internalPool yourself.
     */
    public AbstractThriftPool() {}

    @Override
    public void close() {
        closeInternalPool();
    }

    public boolean isClosed() {
        return this.internalPool.isClosed();
    }

    public AbstractThriftPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
        initPool(poolConfig, factory);
    }

    public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
        if (this.internalPool != null) {
            try {
                closeInternalPool();
            } catch (Exception e) {
            }
        }

        this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
    }

    public T getResource() throws ThriftPoolException {
        try {
            return internalPool.borrowObject();
        } catch (Exception e) {
            throw new ThriftPoolException("Could not get a resource from the pool", e);
        }
    }

    protected void returnResourceObject(final T resource) throws ThriftPoolException {
        if (resource == null) {
            return;
        }
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new ThriftPoolException("Could not return the resource to the pool", e);
        }
    }

    protected void returnBrokenResource(final T resource) throws ThriftPoolException {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    protected void returnResource(final T resource) throws ThriftPoolException {
        if (resource != null) {
            returnResourceObject(resource);
        }
    }

    public void destroy() {
        closeInternalPool();
    }

    protected void returnBrokenResourceObject(final T resource) throws ThriftPoolException {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new ThriftPoolException("Could not return the resource to the pool", e);
        }
    }

    protected void closeInternalPool() throws ThriftException {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new ThriftException("Could not destroy the pool", e);
        }
    }

    public int getNumActive() {
        if (this.internalPool == null || this.internalPool.isClosed()) {
            return -1;
        }

        return this.internalPool.getNumActive();
    }

    public int getNumIdle() {
        if (this.internalPool == null || this.internalPool.isClosed()) {
            return -1;
        }

        return this.internalPool.getNumIdle();
    }

    public int getNumWaiters() {
        if (this.internalPool == null || this.internalPool.isClosed()) {
            return -1;
        }

        return this.internalPool.getNumWaiters();
    }

    public void addObjects(int count) throws ThriftPoolException {
        try {
            for (int i = 0; i < count; i++) {
                this.internalPool.addObject();
            }
        } catch (Exception e) {
            throw new ThriftPoolException("Error trying to add idle objects", e);
        }
    }
}
