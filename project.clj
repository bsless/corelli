(defproject corelli "0.0.1-alpha"
  :description "Compose core.async processes"
  :url "https://github.com/bsless/corelli"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "0.4.500"]
                 [org.clojure/spec.alpha "0.2.176"]]
  :repl-options {:init-ns corelli.core})
