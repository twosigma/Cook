clj-http-async-pool
===================

pooling middleware for async clj-http requests

Usage
-----

    (use '[clj-http-async-pool.router :as http-router])
    (def router (http-router/make-router {:hosts #{"www.random.org:80"}}))
    (use '[clj-http-async-pool.client :as http])
    (http/get router "https://www.random.org/sequences/?min=1&max=42&col=1&format=plain")

Documentation
-------------

Check the docstrings in [router.clj](https://gitlab.twosigma.com/simulations/clj-http-async-pool/blob/master/src/clj_http_async_pool/router.clj), [circuit_breaker.clj](https://gitlab.twosigma.com/simulations/clj-http-async-pool/blob/master/src/clj_http_async_pool/circuit_breaker.clj), and [pool.clj](https://gitlab.twosigma.com/simulations/clj-http-async-pool/blob/master/src/clj_http_async_pool/pool.clj).

© Two Sigma Open Source, LLC
