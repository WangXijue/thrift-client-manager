package com.didi.thrift.pool.exceptions;

public class ThriftConnectionException extends RuntimeException {
  private static final long serialVersionUID = 3878126572474819403L;

  public ThriftConnectionException(String message) {
    super(message);
  }

  public ThriftConnectionException(Throwable cause) {
    super(cause);
  }

  public ThriftConnectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
