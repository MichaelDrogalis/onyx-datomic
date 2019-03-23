(defproject org.onyxplatform/onyx-datomic "0.9.15.3-SNAPSHOT"
  :description "Onyx plugin for Datomic"
  :url "https://github.com/onyx-platform/onyx-datomic"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.9.15"]
                 [aero "0.2.0"]]
  :test-selectors {:default (complement :ci)
                   :ci :ci
                   :all (constantly true)}
  :profiles {:dev {:dependencies [[com.datomic/datomic-free "0.9.5394"]]
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]
                   :resource-paths ["test-resources/"]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
