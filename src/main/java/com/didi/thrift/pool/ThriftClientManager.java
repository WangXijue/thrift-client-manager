package com.didi.thrift.pool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
//import com.didi.thrift.pool.HostAndPort;
import org.slf4j.LoggerFactory;

import com.didi.thrift.pool.ThriftClientFactory.ClientFactory;
import com.didi.thrift.pool.exceptions.ThriftPoolException;

public class ThriftClientManager<T extends TServiceClient> {
    private static Logger logger = LoggerFactory.getLogger(ThriftClientManager.class);
    
    protected final ArrayList<ThriftPool<T>> serverPartitions = new ArrayList<ThriftPool<T>>();
    private BlockingQueue<ThriftPool<T>> suspectedServers = new LinkedBlockingQueue<>(); 
    protected final ServerLoadBalancer<T> loadBalancer;
    private final Thread serverChecker = new CheckerThread(2);
    private volatile boolean stop;
    
    /**
     * Create a ThriftClientManager, use default pool config.
     * @param clientFactory - ClientFactory to create thrift clients
     * @param servers - Server address list
     */
    public ThriftClientManager(ClientFactory<T> clientFactory, List<HostAndPort> servers) {
        this(clientFactory, servers, new GenericObjectPoolConfig());
    }
    
    public ThriftClientManager(ClientFactory<T> clientFactory, List<HostAndPort> servers, GenericObjectPoolConfig poolConfig) {
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("At least one server is required");
        }
        for (HostAndPort addr : servers) {
            serverPartitions.add(new ThriftPool<T>(clientFactory, poolConfig, addr.getHost(), addr.getPort()));
        }
        loadBalancer = new ServerLoadBalancer<T>(serverPartitions);
    }
    
    // XXX: Do NOT start threads in constructor!
    public void init() {
        serverChecker.start();
    }
    
    public T getThriftClient() throws ThriftPoolException {
        ThriftPool<T> pickedPool = loadBalancer.pickServer();
        return pickedPool.getResource();
    }
    
    public void close() {
        stop = true;
        try {
            serverChecker.join();
        } catch (InterruptedException e) {
            // Non-graceful shutdown occurred
            logger.error("Interrupted while joining threads!", e);
        }
        
        for (ThriftPool<T> p : serverPartitions) {
            p.close();
        }
    }
    
    public class CheckerThread extends Thread {
        private int checkInterval;
        
        public CheckerThread(int checkInterval) {
            this.checkInterval = checkInterval;
        }
        
        @Override
        public void run() {
            Set<ThriftPool<T>> serversShouldRecheck = new HashSet<ThriftPool<T>>();
            
            while (!stop) {
                // If suspected server list is not empty, then give priority to the suspected servers.
                while (!suspectedServers.isEmpty()) {
                    ThriftPool<T> server = suspectedServers.poll();
                    logger.info("Check suspected server <{}>", server);
                    if (!server.checkServer()) {
                        logger.info("Suspected server <{}> should be check again", server);
                        serversShouldRecheck.add(server);
                    }
                }
                
                // Normal check routine
                for (ThriftPool<T> svr : serverPartitions) {
                    if (!svr.checkServer()) {
                        logger.info("Server <{}> is suspected", svr);
                        serversShouldRecheck.add(svr);
                    }
                }
                
                // Servers should be rechecked in the next period
                suspectedServers.addAll(serversShouldRecheck);
                serversShouldRecheck.clear();
                
                try {
                    Thread.sleep(checkInterval * 1000);
                } catch (InterruptedException e) {
                    // Handle the exception.
                    Thread.currentThread().interrupt();
                }     
            }
        }
    }// CheckerThread
    
    /**
     * A round robin load balancer for choosing server for new requests.
     * <p>This class is thread-safe.</p>
     */
    protected static class ServerLoadBalancer<T extends TServiceClient> {
        private final ArrayList<ThriftPool<T>> servers;
        private final AtomicInteger idx = new AtomicInteger(0);
        private final int MAX_RETRY = 5;

        public  ServerLoadBalancer(ArrayList<ThriftPool<T>> servers) {
            if (servers.isEmpty()) {
                throw new IllegalArgumentException("At least one server is required");
            }
            this.servers = servers;
        }

        public ThriftPool<T> pickServer() throws ThriftPoolException {
            // TODO All server is down? Cannot get any client?
            for (int tries = 1; tries < MAX_RETRY; tries++) {
                int picked = (idx.getAndIncrement() % servers.size());
                ThriftPool<T> pickedPool = servers.get(picked);
                if (pickedPool.suspected) {
                    continue;
                }
                return pickedPool;
            }
            throw new ThriftPoolException("No server available");
        }
    }//ServerLoadBalancer
}
