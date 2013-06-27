(ns test_project1.user
  (:require [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.nodes :as nn]
            [clojurewerkz.neocons.rest.relationships :as nrl]
            [clojurewerkz.neocons.rest.cypher :as cy]
            [clojure.string :as str]))

(defn -main
  [& args]
  (nr/connect! "http://localhost:7474/db/data/"))
