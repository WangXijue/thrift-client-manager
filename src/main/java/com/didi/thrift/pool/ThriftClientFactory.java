package com.didi.thrift.pool;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.didi.thrift.pool.exceptions.ThriftException;

/**
 * PooledObjectFactory custom impl.
 */
class ThriftClientFactory<T extends TServiceClient> implements PooledObjectFactory<T> {

    private static Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);
    
    private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();
    private final int connectionTimeout;
    private final int soTimeout;

    private ClientFactory<T> clientFactory;
    @SuppressWarnings("unused")
    private ProtocolFactory protocolFactory;    // TODO export protocol factory
    
    public static interface ClientFactory<T> {
        T createClient(TProtocol tProtocol);
    }
    
    public static interface ProtocolFactory {
        TProtocol createProtocol();
    }

    public static class BinarySocketProtocolFactory implements ProtocolFactory {
        private String host;
        private int port;

        public BinarySocketProtocolFactory(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @SuppressWarnings("resource")
        public TProtocol createProtocol() {
            TTransport transport = new TSocket(host, port);
            try {
                transport.open();
            } catch (TTransportException e) {
                //logger.warn("whut?", e);
                throw new ThriftException("Can not create protocol", e);
            }
            return new TBinaryProtocol(transport);
        }
    }

    public ThriftClientFactory(ClientFactory<T> clientFactory, //ProtocolFactory protocolFactory, 
            final String host, final int port, final int connectionTimeout,
            final int soTimeout) {
        this.clientFactory = clientFactory;
        //this.protocolFactory = protocolFactory; TODO
        this.hostAndPort.set(new HostAndPort(host, port));
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
    }

    public void setHostAndPort(final HostAndPort hostAndPort) {
        this.hostAndPort.set(hostAndPort);
    }

    // Reinitialize an client to be returned by the pool.
    @Override
    public void activateObject(PooledObject<T> pooledClient) throws Exception {
        // TODO do nothing
    }

    @Override
    public void destroyObject(PooledObject<T> pooledClient) throws Exception {
        final T client = pooledClient.getObject();
        if (client.getOutputProtocol().getTransport().isOpen()) {
            client.getOutputProtocol().getTransport().close();
        }
        if (client.getInputProtocol().getTransport().isOpen()) {
            client.getInputProtocol().getTransport().close();
        }
    }

    @Override
    public PooledObject<T> makeObject() throws Exception {
        final HostAndPort hostAndPort = this.hostAndPort.get();
        TTransport transport = new TFramedTransport(new TSocket(hostAndPort.getHost(), hostAndPort.getPort(),
                soTimeout, connectionTimeout));
        //TTransport transport = new TSocket("localhost", 5681);
        
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new DefaultPooledObject<T>(clientFactory.createClient(protocol));
    }

    @Override
    public void passivateObject(PooledObject<T> pooledTransport) throws Exception {
        // TODO Not sure right now. Close the transport for this client?
    }

    @Override
    public boolean validateObject(PooledObject<T> pooledTransport) {
        try {
            T client = pooledTransport.getObject();
            // TODO 需要验证host & port 与原来是否一致？
            if (client instanceof PooledClient) {
                PooledClient pClient = (PooledClient) client;
                String pingReply = pClient.ping("PING");
                if (pingReply != "PONG") {
                    logger.warn("Ping server returns unexpected reply: {}", pingReply);
                }
            }
        } catch (Exception e) {
            logger.error("Check server failed", e);
            return false;
        }
        
        return true;
    }
}