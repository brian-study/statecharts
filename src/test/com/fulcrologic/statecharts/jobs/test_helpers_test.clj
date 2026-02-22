(ns com.fulcrologic.statecharts.jobs.test-helpers-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.statecharts.jobs.test-helpers :as th]))

(deftest make-delay-handler-without-tracker-succeeds-test
  (testing "make-delay-handler succeeds when :tracker is omitted"
    (let [handler (th/make-delay-handler 0)
          result (handler {:job-id :delay-job
                           :params {:job-index 7}
                           :continue-fn (constantly true)})]
      (is (= {:job-id :delay-job
              :job-index 7
              :delay-ms 0
              :continue? true}
             result)))))

(deftest make-failing-handler-without-tracker-succeeds-test
  (testing "make-failing-handler succeeds when :tracker is omitted"
    (let [handler (th/make-failing-handler {:delay-ms 0
                                            :fail-job-indexes #{}})
          result (handler {:job-id :failing-job
                           :params {:job-index 2}
                           :continue-fn (constantly true)})]
      (is (= {:job-id :failing-job
              :job-index 2
              :continue? true}
             result)))))

(deftest make-slow-handler-without-tracker-succeeds-test
  (testing "make-slow-handler succeeds when :tracker is omitted"
    (let [handler (th/make-slow-handler 0)
          result (handler {:job-id :slow-job
                           :continue-fn (constantly true)})]
      (is (= {:job-id :slow-job
              :worker "slow"
              :continue-after-sleep true}
             result)))))
