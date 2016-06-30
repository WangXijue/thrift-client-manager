package com.didi.thrift.pool.exceptions;

public class ThriftException extends RuntimeException {

    public ThriftException(String message, Exception e) {
        super(message, e);
    }

    /**
     * 
     */
    private static final long serialVersionUID = -6219688375186884508L;

}
