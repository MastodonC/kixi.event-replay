(defproject kixi.event-replay "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[amazonica "0.3.112"]
                 [aero "1.1.2"]
                 [baldr "0.1.1"]
                 [clj-time "0.14.0"]
                 [environ "1.1.0"]
                 [org.clojure/clojure "1.9.0-beta2"]]

  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]]}
             :uberjar {:aot [kixi.event-replay]
                       :uberjar-name "kixi.event-replay-standalone.jar"}}
  :main kixi.event-replay)
