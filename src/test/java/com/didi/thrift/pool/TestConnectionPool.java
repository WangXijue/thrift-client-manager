package com.didi.thrift.pool;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.didi.duse.api.DuseApi;
import com.didi.duse.api.DuseApi.Client;
import com.didi.thrift.pool.HostAndPort;
import com.didi.thrift.pool.PooledClient;
import com.didi.thrift.pool.ThriftClientFactory;
import com.didi.thrift.pool.ThriftClientManager;
import com.didi.thrift.pool.ThriftPool;
import com.didi.thrift.pool.exceptions.ThriftPoolException;

import junit.framework.TestCase;

public class TestConnectionPool extends TestCase {
    ThriftClientManager<PooledDuseApiClient> clientManager;

    /**
     * PooledClient实现样例，继承Thrift编译生成的Client，并实现PooledClient接口
     */
    class PooledDuseApiClient extends DuseApi.Client implements PooledClient {
        @SuppressWarnings("rawtypes")
        protected ThriftPool pool;
        private volatile boolean released;

        public PooledDuseApiClient(TProtocol prot) {
            super(prot);
        }

        /**
         * 实现PooledClient的close接口，在使用完后将此client归还给连接管理器
         */
        @SuppressWarnings("unchecked")
        @Override
        public void release() {
            if (released) {
                return;
            }
            released = true;
            pool.returnResource(this);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void releaseBroken(Exception e) {
            if (released) {
                return;
            }
            released = true;
            pool.returnBrokenResource(this, e);
        }

        /**
         * 实现ping接口，让连接管理器进行服务健康检查
         */
        @Override
        public String ping(String s) throws TException {
            return Ping(s);
        }

        @Override
        public void setupPooledClient(@SuppressWarnings("rawtypes") ThriftPool p) {
            this.pool = p;
            released = false;
        }
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        List<HostAndPort> serverAddrList = new ArrayList<HostAndPort>();
        serverAddrList.add(new HostAndPort("localhost", 5681));

        ThriftClientFactory.ClientFactory<PooledDuseApiClient> clientFactory = new ThriftClientFactory.ClientFactory<PooledDuseApiClient>() {
            @Override
            public PooledDuseApiClient createClient(TProtocol tProtocol) {
                return new PooledDuseApiClient(tProtocol);
            }
        };

        clientManager = new ThriftClientManager<PooledDuseApiClient>(clientFactory, serverAddrList);
        // clientManager.init();
    }

    public void testPooledClient() throws Exception {
        PooledDuseApiClient client = null;
        for (int i = 0; i < 5000; i++) {
            try {
                client = clientManager.getThriftClient();
                // System.out.println(client); // Print this client's reference
                String reply = client.Ping("PING");
                assertEquals("PONG", reply);
                // System.out.println("PASS");
            } catch (TTransportException e) {
                client.releaseBroken(e);
            } catch (ThriftPoolException e) {
                e.printStackTrace();
            } finally {
                if (client != null) {
                    client.release();
                }
            }
        }
        // Thread.sleep(30 * 1000);
    }

    public void testShortConnection() throws Exception {
        TTransport transport = null;
        for (int i = 0; i < 5000; i++) {
            try {
                transport = new TFramedTransport(new TSocket("localhost", 5681));
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);

                Client.Factory clientFactory = new Client.Factory();
                Client client = clientFactory.getClient(protocol);
                String reply = client.Ping("PING");
                assertEquals("PONG", reply);
            } finally {
                if (transport != null && transport.isOpen()) {
                    transport.close();
                }
            }
        }
        // Thread.sleep(5000);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        clientManager.close();
    }

}
