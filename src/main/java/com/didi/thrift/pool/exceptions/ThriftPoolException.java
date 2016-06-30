package com.didi.thrift.pool.exceptions;

public class ThriftPoolException extends Exception {
    
    private static final long serialVersionUID = 1537630778484947796L;

    public ThriftPoolException(String string) {
        super(string);
    }

    public ThriftPoolException() {
        super();
    }

    public ThriftPoolException(Throwable cause) {
        super(cause);
    }

    public ThriftPoolException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
