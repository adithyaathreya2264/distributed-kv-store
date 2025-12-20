package com.dkv.client;

public class KVClientException extends RuntimeException {
    public KVClientException(String message) {
        super(message);
    }

    public KVClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
