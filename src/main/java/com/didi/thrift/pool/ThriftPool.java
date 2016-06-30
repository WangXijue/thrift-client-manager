package com.didi.thrift.pool;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.didi.thrift.pool.ThriftClientFactory.ClientFactory;
import com.didi.thrift.pool.exceptions.ThriftException;
import com.didi.thrift.pool.exceptions.ThriftPoolException;

/**
 * Thrift Client pool for single server host
 * @author OutOfMemory
 *
 * @param <T> extends TServiceClient
 */
public class ThriftPool<T extends TServiceClient> extends AbstractThriftPool<T> {
    private static Logger logger = LoggerFactory.getLogger(ThriftPool.class);

    // TODO
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5681;
    public static final int DEFAULT_TIMEOUT = 2000;
    private final HostAndPort serverAddr;
    
    protected volatile boolean suspected;   // TODO Volatile -> AtomicBoolean?
    
    public ThriftPool(ClientFactory<T> clientFactory) {
        this(clientFactory, DEFAULT_HOST, DEFAULT_PORT);
    }

    public ThriftPool(ClientFactory<T> clientFactory, String host, int port) {
        this(clientFactory, new GenericObjectPoolConfig(), host, port, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT);
    }

    public ThriftPool(ClientFactory<T> clientFactory, final GenericObjectPoolConfig poolConfig, final String host, final int port) {
        this(clientFactory, poolConfig, host, port, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT);
    }

    public ThriftPool(ClientFactory<T> clientFactory,final GenericObjectPoolConfig poolConfig, final String host, int port,
            final int connectionTimeout, final int soTimeout) {
        super(poolConfig, new ThriftClientFactory<T>(clientFactory, host, port, connectionTimeout, soTimeout));
        serverAddr = new HostAndPort(host, port);
    }

    /**
     * 
     * @return Returns true if the server is OK (Connected and reply PING).
     */
    public boolean checkServer() {
        try {
            T client = super.internalPool.getFactory().makeObject().getObject();
            if (client instanceof PooledClient) {
                PooledClient pClient = (PooledClient) client;
                String pingReply = pClient.ping("PING");
                if (!pingReply.equals("PONG")) {
                    logger.warn("Ping server returns unexpected reply: {}", pingReply);
                }
            }
            
            if (client.getOutputProtocol().getTransport().isOpen()) {
                client.getOutputProtocol().getTransport().close();
            }
            if (client.getInputProtocol().getTransport().isOpen()) {
                client.getInputProtocol().getTransport().close();
            }
            suspected = false;
            return true;
        } catch (Exception e) {
            logger.error("Check server failed", e);
            suspected = true;
            return false;
        }
    }
    
    public HostAndPort getHostAndPort() {
        return serverAddr;
    }
    
    @Override
    public T getResource() throws ThriftPoolException {
        T thriftClient = super.getResource();
        // TODO thriftClient.setDataSource(this);
        if (thriftClient instanceof PooledClient) {
            PooledClient pooledClient = (PooledClient)thriftClient;
            pooledClient.setupPooledClient(this);
        }
        return thriftClient;
    }

    @Override
    protected void returnBrokenResource(final T resource) {
        if (resource != null) {
            try {
                super.returnBrokenResourceObject(resource);
            } catch (ThriftPoolException e) {
                throw new ThriftException("Could not return the resource to the pool", e);
            }
        }
    }

    @Override
    protected void returnResource(final T resource) {
        if (resource != null) {
            try {
                // resource.resetState();
                returnResourceObject(resource);
            } catch (Exception e) {
                returnBrokenResource(resource);
                throw new ThriftException("Could not return the resource to the pool", e);
            }
        }
    }
    
    @Override
    public String toString() {
        // TODO
        return serverAddr.toString();
    }

    // TODO notify checker thread to check me
    public void returnBrokenResource(final T resource, Exception e) {
        if (e instanceof TTransportException) {
            TTransportException transportException = (TTransportException) e;
            logger.info("returnBrokenResource with exception", e);

            switch (transportException.getType()) {
            case TTransportException.END_OF_FILE:
                System.err.println("Broken connection");
                returnBrokenResource(resource);
                break;
            case TTransportException.UNKNOWN:
                // Caused by java.io.IOException, just close transport of this client
                System.err.println("Broken pipe");
                returnBrokenResource(resource);
                break;
            case TTransportException.TIMED_OUT:
                returnResource(resource);
                break;
            default:
                // Should never go to here
                logger.warn("returnBrokenResource with unknown exception", e);
                returnResource(resource);
            }

            return;
        }

        // Should never go to here
        logger.warn("returnBrokenResource with unknown exception", e);
        returnResource(resource);
    }
}
