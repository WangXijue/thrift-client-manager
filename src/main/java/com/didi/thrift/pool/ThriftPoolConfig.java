package com.didi.thrift.pool;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ThriftPoolConfig extends GenericObjectPoolConfig {
    public ThriftPoolConfig() {
        // defaults to make your life with connection pool easier :)
        setTestWhileIdle(true);
        setMinEvictableIdleTimeMillis(60000);
        setTimeBetweenEvictionRunsMillis(30000);
        setNumTestsPerEvictionRun(-1);
    }
}
