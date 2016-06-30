package com.didi.thrift.pool;

/**
 * Hello world!
 *
 */
public interface PooledClient {
    /**
     * Ping server
     * @param msg - "PING"
     * @return "PONG"
     * @throws org.apache.thrift.TException
     */
    public String ping(String msg) throws org.apache.thrift.TException;
    
    public void setupPooledClient(@SuppressWarnings("rawtypes") ThriftPool p);
    
    /**
     * Release pooled client, i.e. return the client to thrift-pool
     */
    public void release();
    
    /**
     * Release broken client, i.e. connection broken.
     */
    public void releaseBroken(Exception e);

}
