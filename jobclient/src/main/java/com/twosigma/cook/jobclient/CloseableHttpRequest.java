package com.twosigma.cook.jobclient;

import org.apache.http.client.methods.HttpRequestBase;

import java.io.Closeable;
import java.io.IOException;

class CloseableHttpRequest<T extends HttpRequestBase> implements Closeable {
    private final T _request;
    public CloseableHttpRequest(T request) {
        _request = request;
    }

    public T get() {
        return _request;
    }

    @Override
    public void close() throws IOException {
        if (_request != null)
            _request.releaseConnection();
    }
}

