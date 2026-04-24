(ns build
  "Build a jar of com.fulcrologic/statecharts using tools.build.

   Usage:
     clojure -T:build jar        ;; produces target/statecharts-<version>.jar
     clojure -T:build clean      ;; wipe target/"
  (:require
   [clojure.tools.build.api :as b]))

(def lib 'com.fulcrologic/statecharts)
(def version "2.0.11")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src/main"]
               :target-dir class-dir})
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src/main"]
                :scm {:url "https://github.com/brian-study/statecharts"
                      :connection "scm:git:git://github.com/brian-study/statecharts.git"
                      :developerConnection "scm:git:ssh://git@github.com/brian-study/statecharts.git"
                      :tag (str "statecharts-" version)}})
  (b/jar {:class-dir class-dir
          :jar-file jar-file})
  (println "Wrote" jar-file))
