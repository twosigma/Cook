(ns cook.test.rest.cors
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [cook.rest.cors :as cors]))


(deftest test-is-preflight?
  (is (cors/preflight? {:request-method :options}))
  (is (not (cors/preflight? {:request-method :get})))
  (is (not (cors/preflight? {:request-method :put}))))

(deftest test-same-origin?
  (is (cors/same-origin? {:headers {"host" "example.com"
                                    "origin" "http://example.com"}
                          :scheme :http}))
  (is (cors/same-origin? {:headers {"host" "example.com"
                                    "origin" "https://example.com"
                                    "x-forwarded-proto" "https"}
                          :scheme :http}))
  (is (not (cors/same-origin? {:headers {"host" "example.com"
                                         "origin" "https://example.com"
                                         "x-forwarded-proto" "http"}
                               :scheme :https})))
  (is (not (cors/same-origin? {:headers {"host" "example.com"}
                               :scheme :http})))
  (is (not (cors/same-origin? {:headers {"origin" "http://bad.example.com"
                                         "host" "example.com"}
                               :scheme :http})))
  (is (not (cors/same-origin? {:headers {"origin" "http://example.com"
                                         "host" "example.com"}
                               :scheme :https}))))

(deftest test-request-allowed?
  (let [allowed-orgins [#"^https?://example.com$"
                        #"^https://secure.example.com"]
        origin-allowed? (fn [origin] (cors/request-allowed? {:headers {"origin" origin
                                                                       "host" "other.example.com"}}
                                                           allowed-orgins))]
    (is (origin-allowed? "http://example.com"))
    (is (origin-allowed? "https://example.com"))
    (is (origin-allowed? "https://secure.example.com"))
    (is (not (origin-allowed? "http://secure.example.com")))
    (is (cors/request-allowed? {:headers {"origin" "http://same.example.com"
                                          "host" "same.example.com"}
                                :scheme :http}
                               []))))

(deftest test-wrap-preflight
  (let [default-resp {:status 200 :body "default"}
        allowed-origins [#"http://example.com"]
        handler (cors/wrap-preflight (fn [_] default-resp) allowed-origins)]
    (testing "passes through non-preflight requests"
      (is (= default-resp (handler {:request-method :get
                                    :headers {"origin" "http://bad.example.com"}}))))

    (testing "denies disallowed preflight requests"
      (let [{:keys [status body]} (handler {:request-method :options
                                            :headers {"origin" "http://bad.example.com"}})]
        (is (= 403 status))
        (is (str/includes? body "http://bad.example.com"))))

    (testing "allows valid preflight requests"
      (let [{:keys [status headers]} (handler {:request-method :options
                                               :headers {"origin" "http://example.com"
                                                         "access-control-request-headers" "foo,bar"}})]
        (is (= 200 status))
        (is (= "http://example.com" (headers "Access-Control-Allow-Origin")))
        (is (= "foo,bar" (headers "Access-Control-Allow-Headers")))
        (is (every? #(str/includes? (headers "Access-Control-Allow-Methods") %)
                    ["PUT" "GET" "OPTIONS" "DELETE"]))
        (is (= "true" (headers "Access-Control-Allow-Credentials")))
        (is (= "86400" (headers "Access-Control-Max-Age")))))))


(deftest test-wrap-cors
  (let [default-resp {:status 200 :body "default"}
        allowed-origins [#"http://example.com"]
        handler (cors/wrap-cors (fn [{:keys [headers]}] (assoc default-resp :headers headers))
                                allowed-origins)]
    (testing "reqeusts without origin pass through"
      (let [{:keys [status body headers]} (handler {:request-method :get
                                                    :headers {"foo" "bar"}})]
        (is (= 200 status))
        (is (= "default" body))
        (is (= {"foo" "bar"} headers))))

    (testing "deny requests from disallowed origin"
      (let [{:keys [status body]} (handler {:request-method :get
                                            :headers {"origin" "http://bad.example.com"}})]
        (is (= 403 status))
        (is (str/includes? body "http://bad.example.com"))))

    (testing "adds cors headers on requests from allowed origin"
      (let [req-headers {"foo" "bar"
                         "origin" "http://example.com"}
            {:keys [status body headers]} (handler {:request-method :get
                                                    :headers req-headers})]
        (is (= 200 status))
        (is (= "default" body))
        (is (= (assoc req-headers "Access-Control-Allow-Origin" "http://example.com"
                      "Access-Control-Allow-Credentials" "true")))))))
