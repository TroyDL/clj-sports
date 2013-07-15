;; linum-mode
(ns user
  (:require [clojure.pprint :refer (pprint pp)]
            [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.nodes :as nn]
            [clojurewerkz.neocons.rest.relationships :as nrl]
            [clojurewerkz.neocons.rest.cypher :as cy]
            [clojure.string :as str]
            [clojure.set :as set]
            [korma.db :as kdb]
            [korma.core :as kc]))

(kdb/defdb mydb
  (kdb/mysql {:db "blitz"
              :user "root"
              :password "root"}))

(nr/connect! "http://localhost:7474/db/data/")

(def relationships
  {:HAS_SCORE_TYPE "Sport to score-type"
   :HAS_ACTION_TYPE "Sport to action-type"
   :HAS_PARENT_TYPE "Action-type to action-type"
   :HAS_STAT_TYPE "Sport to stat-type"
   :HAS_POSITION "Sport to position"
   :HAS_SEASON "Sport to season"
   :HAS_LEAGUE "Sport to league"
   :COMPETES_IN "Team to sport"
   :REPRESENTS "Team to school"
   :HAS_LEAGUE_SEASON "League to league-season"
   :DURING_SEASON "League-season to season"
   :IN_SECTION "League-season to section"
   :HAD_SEASON "Team to team-season"
   :COMPETED_IN_SEASON "Team-season to season"
   :COMPETED_IN_LEAGUE "Team-season to league-season"
   :COMPETED_IN_DIVISION "Team-season to division"
   :HAD_MATCHUP "Season to game"
   :COMPETED_IN "Team to game"})

(def index-map
  {"sport_idx" "slug"
   "position_idx" "name"
   "score_type_idx" "name"
   "action_type_idx" "name"
   "stat_type_idx" "name"
   "game_type_idx" "name"
   "coach_idx" "slug"
   "student_idx" "slug"
   "school_idx" "slug"
   "season_idx" "name"
   "league_idx" "name"})

(def url-re
  #"(?:https?:\/\/)?(?:[\da-z\.-]+)\.(?:[a-z\.]{2,6})(?:[\/\w \.-]*)*")

(declare
 sql-player                      sql-coach
 sql-action                      sql-action-type
 sql-action-type-score-type      sql-coach-team-season
 sql-game                        sql-game-type
 sql-league                      sql-league-season
 sql-matchup                     sql-player-team-season
 sql-player-team-season-position sql-position
 sql-school                      sql-score
 sql-score-type                  sql-season
 sql-sport                       sql-stat
 sql-stat-type                   sql-team
 sql-team-season)

(kc/defentity sql-action
  (kc/table "action")
  (kc/belongs-to sql-game)
  (kc/belongs-to sql-team)
  (kc/belongs-to sql-action-type)
  (kc/belongs-to sql-score-type))

(kc/defentity sql-action-type
  (kc/table "action_type")
  (kc/pk "action_type_id")
  (kc/belongs-to sql-sport)
  (kc/has-many sql-action)
  (kc/has-many sql-action-type-score-type))

(kc/defentity sql-action-type-score-type
  (kc/table "action_type_score_type")
  (kc/belongs-to sql-action-type)
  (kc/belongs-to sql-score-type))

(kc/defentity sql-coach
  (kc/table "coach")
  (kc/has-many sql-coach-team-season))

(kc/defentity sql-coach-team-season
  (kc/table "coach_team_season")
  (kc/belongs-to sql-coach)
  (kc/belongs-to sql-team))

(kc/defentity sql-game
  (kc/table "game")
  (kc/pk "game_id")
  (kc/belongs-to sql-season)
  (kc/belongs-to sql-sport)
  (kc/belongs-to sql-game-type)
  (kc/belongs-to sql-school)
  (kc/has-many sql-action)
  (kc/has-many sql-score)
  (kc/has-many sql-stat)
  (kc/has-one sql-matchup))

(kc/defentity sql-game-type
  (kc/table "game_type")
  (kc/pk "game_type_id")
  (kc/has-many sql-game))

(kc/defentity sql-league
  (kc/table "league")
  (kc/pk "league_id")
  (kc/belongs-to sql-sport)
  (kc/has-many sql-league-season)
  (kc/has-many sql-team-season))

(kc/defentity sql-league-season
  (kc/table "league_season")
  (kc/pk "league_season_id")
  (kc/belongs-to sql-league)
  (kc/belongs-to sql-season))

(kc/defentity sql-matchup
  (kc/table "matchup")
  (kc/has-many sql-matchup)
  (kc/belongs-to sql-game)
  (kc/belongs-to sql-team))

(kc/defentity sql-player
  (kc/table "player")
  (kc/pk "player_id")
  (kc/has-many sql-stat))

(kc/defentity sql-player-team-season
  (kc/table "player_team_season")
  (kc/belongs-to sql-player)
  (kc/belongs-to sql-team)
  (kc/belongs-to sql-season))

(kc/defentity sql-player-team-season-position
  (kc/table "player_team_season_position")
  (kc/belongs-to sql-player-team-season)
  (kc/belongs-to sql-position))

(kc/defentity sql-position
  (kc/table "position")
  (kc/pk "position_id")
  (kc/belongs-to sql-sport))

(kc/defentity sql-school
  (kc/table "school")
  (kc/pk "school_id")
  (kc/has-many sql-game)
  (kc/has-one sql-team))

(kc/defentity sql-score
  (kc/table "score")
  (kc/belongs-to sql-game)
  (kc/belongs-to sql-team))

(kc/defentity sql-score-type
  (kc/table "score_type")
  (kc/pk "score_type_id")
  (kc/has-many sql-action)
  (kc/has-many sql-action-type-score-type))

(kc/defentity sql-season
  (kc/table "season")
  (kc/pk "season_id")
  (kc/belongs-to sql-sport)
  (kc/has-many sql-coach-team-season)
  (kc/has-many sql-game)
  (kc/has-many sql-league-season)
  (kc/has-many sql-team-season))

(kc/defentity sql-sport
  (kc/table "sport")
  (kc/pk "sport_id")
  (kc/has-many sql-action-type)
  (kc/has-many sql-league)
  (kc/has-many sql-game)
  (kc/has-many sql-position)
  (kc/has-many sql-season)
  (kc/has-many sql-stat-type)
  (kc/has-many sql-team))

(kc/defentity sql-stat
  (kc/table "stat")
  (kc/pk "stat_id")
  (kc/belongs-to sql-stat-type)
  (kc/belongs-to sql-game)
  (kc/belongs-to sql-player)
  (kc/belongs-to sql-team))

(kc/defentity sql-stat-type
  (kc/table "stat_type")
  (kc/pk "stat_type_id")
  (kc/belongs-to sql-sport)
  (kc/has-many sql-stat))

(kc/defentity sql-team
  (kc/table "team")
  (kc/pk "team_id")
  (kc/belongs-to sql-school)
  (kc/belongs-to sql-sport)
  (kc/has-many sql-action)
  (kc/has-many sql-coach-team-season)
  (kc/has-many sql-matchup)
  (kc/has-many sql-score)
  (kc/has-many sql-stat)
  (kc/has-many sql-team-season))

(kc/defentity sql-team-season
  (kc/table "team_season")
  (kc/belongs-to sql-team)
  (kc/belongs-to sql-season)
  (kc/belongs-to sql-league))

(defn check-index
  "Helper for query function.  Formats an index
   string to fit in a NeoCons query or defaults
   to no index."
  [index-str]
  (if (not= index-str "(*)")
    (str \: index-str "('"
         (index-map index-str) ":*')")
    "(*)"))

(defn compile-query
  "Helper for query function.  Compiles query string.  Nodes only."
  [index key val limit]
  (cond (= "" val)
        (format "start n=node%s where HAS (n.%s) return n limit %d"
                index key limit)
        (string? val)
        (format "start n=node%s where HAS (n.%s) AND n.%s=~ '(?i).*%s.*' return n limit %d"
                index key key val limit)
        (not (string? val))
        (format "start n=node%s where HAS (n.%s) AND n.%s=%d return n limit %d"
                index key key val limit)))

(defn pull-from
  "Helper for query function.  Takes a list of maps
   and returns a list of the values of the given key."
  [map-list ret]
  (loop [maps map-list
         res '()]
    (if-let [map (first maps)]
      (recur (rest maps)
             (conj res (ret map)))
      res)))

(defn id-of
  "This will return the neo4j id of a node or relationship from
   the return values of get, query or tquery."
  [node]
  (cond (or (= (type node) clojurewerkz.neocons.rest.records.Relationship)
            (= (type node) clojurewerkz.neocons.rest.records.Node))
          (:id node)
        (= (type node) clojurewerkz.neocons.rest.records.CypherQueryResponse)
          (Integer. (second (str/split
                           (:self (first (first (:data node))))
                           #"node/|relationship/")))
        (= (type node) clojure.lang.LazySeq)
          (Integer. (second (str/split
                           (or (get-in (first node) ["n" :self])
                               (get-in (first node) ["r" :self]))
                           #"node/|relationship/")))
        (= (type node) clojure.lang.PersistentHashMap)
          (Integer. (second (str/split (:self node)
                                     #"node/|relationship/")))
        (= (type node) clojure.lang.PersistentVector)
          (Integer. (second (str/split (:self (first node))
                                     #"node/|relationship/")))))

(defn trim-node
  "Trims typically unwanted data from a get, query or tquery response of
   neocons.  Does not work on relationships; see trim-rel.
   --Now works with b-nodes."
  [node]
  (cond (= (type node) clojurewerkz.neocons.rest.records.Node)
        (assoc (:data node) :node_id (:id node))
        (= (type node) clojurewerkz.neocons.rest.records.CypherQueryResponse)
        (assoc 
            (:data (first (first (:data node))))
          :node_id 
          (id-of node))
        (= (type node) clojure.lang.LazySeq)
        (assoc
            (get-in (first node) ["n" :data])
          :node_id
          (id-of node))
        (= (type node) clojure.lang.PersistentVector)
        (assoc
            (:data (first node)) :node_id (id-of node))))

(defn trim-rel
  "TODO"
  [rel]
  (cond (= (type rel) clojurewerkz.neocons.rest.records.Relationship)
        "todo"
        (= (type rel) clojurewerkz.neocons.rest.records.CypherQueryResponse)
        "todo"
        (= (type rel) clojure.lang.LazySeq)
        "todo"))

(defn delete-nodes
  "Takes a list of nodes and sequentially deletes them from the
   neo4j database with neocons."
  [nodes]
  (nn/delete (nn/get (first nodes)))
  (if (next nodes)
    (delete-nodes (next nodes))))

(defn destroy-nodes
  "Takes a list of nodes and sequentially destroys them from neo4j.
   Differs from delete-nodes in that it also removes relationships
   if the node has any.  Delete will refuse to erase a node with
   a relationship."
  [nodes]
  (nn/destroy (nn/get (first nodes)))
  (if (next nodes)
    (destroy-nodes (next nodes))))

(defn query
  "This is a general purpose Neo4J query function.
   Returns all nodes in a graph that have the given
   property.  Map options are :index, :val, :limit
   :return.  Restrict to an index or property value
   and return a list of a particular property.
   Limit default is 10.  Result contains count meta.
   --Does not work for relationships."
  ([property]
     (query property {}))
  ([property option-map]
     (let [property (name property)
           {index :index, val :val
            limit :limit, ret :return
            count? :count 
            :or {index "(*)", val ""
                 ret "all"
                 count? false
                 limit 10}} option-map
           idx (check-index index)
           limit (if count? 999999 limit)
           query (compile-query idx property val limit)] 
       (loop [curr-seq (:data (cy/query query))
              res '()
              node-count 0]
         (if-let [curr (first (first curr-seq))]
           (recur (rest curr-seq)
                  (conj res (assoc (:data curr) :node_id
                               (Integer. (second
                                 (str/split (:self curr) #"node/")))))
                  (inc node-count)) 
           (if count? (println node-count)
             (let [res' (vary-meta res assoc
                                   :count
                                   (str (count res)
                                        " of limit(" limit ")"
                                        " records returned."))]
               (println (str (count res)
                             " of limit(" limit ")"
                             " records returned."))
               (if (= ret "all")
                 res'
                 (pull-from res' (keyword ret))))))))))

(defn rel-query
  "TODO"
  [rel-property limit]
  ())

(defn count-index
  "Convenience function: Takes an index name and returns all
   nodes that index points to.  Uses query function."
  [index]
  (query (index-map index) {:index index :count true}))

(defn find-all-rel-properties-helper [skip]
  (loop [rels (:data
                (cy/query
                 (str "start r=rel(*) return r skip " skip " limit 1000")))
             properties ()]
           (if-let [rel (:data (first (first rels)))]
                   (recur (rest rels)
                          (seq (set (flatten (conj properties (keys rel))))))
                   properties)))

(defn find-all-rel-properties
  "Returns a set of all properties from all relationships in a
   Neo4J graph via NeoCons library."
  []
  (let [rel-count
        (first (first (:data (cy/query "start r=rel(*) return count(r)"))))] 
    (loop [rel-set ()
           skip 0]
      (if (< skip rel-count)
        (recur (seq (set (flatten (conj rel-set
                                        (find-all-rel-properties-helper skip)))))
               (+ skip 1000))
        rel-set))))

(defn find-all-properties-helper [skip]
  (loop [nodes (:data
                (cy/query
                 (str "start n=node(*) return n skip " skip " limit 1000")))
             properties ()]
           (if-let [node (:data (first (first nodes)))]
                   (recur (rest nodes)
                          (seq (set (flatten (conj properties (keys node))))))
                   properties)))

(defn find-all-properties []
  "Returns a set of all properties from all nodes in a
   Neo4J graph via NeoCons library."
  (let [node-count
        (first (first (:data (cy/query "start n=node(*) return count(n)"))))] 
    (loop [node-set ()
           skip 0]
      (if (< skip node-count)
        (recur (seq (set (flatten
                          (conj node-set
                                (find-all-properties-helper skip)))))
               (+ skip 1000))
        node-set))))

(defn create-unique-index [name]
  (try (nn/create-index name {:unique true})
       (catch Exception e)))

(defn get-by-sql-id [index-name id]
  (let [query (str "start n=node:" index-name "('id:" id "') return n") 
        node-dump (cy/query query)]
    (nn/get (:node_id (trim-node node-dump)))))

(defn get-node-betwixt ;;working
  ([node1 node2] (get-node-betwixt node1 node2 "" ""))
  ([node1 node2 rel1 rel2]
     (let [rel1 (if (not= rel1 "") (str \: rel1) "")
           rel2 (if (not= rel2 "") (str \: rel2) "")] 
       (cy/query (format "start n=node(%d),n2=node(%d)
                                match (n)-[%s]-(bnode)-[%s]-(n2)
                                return bnode" node1 node2 rel1 rel2)))))

(defn select-betwixt [bnode node_type]
  (let [nodes (second (first bnode))]
    (loop [n nodes] (cond (= (:node_type (trim-node (first n))) node_type)
                            (trim-node (first n))
                          (next n)
                            (recur (next n))))))

(def league1 (get-by-sql-id "league_id_idx" 18))
(def team1 (get-by-sql-id "team_id_idx" 5))
(def season1 (get-by-sql-id "season_id_idx" 7))
(def league-season1 (get-node-betwixt (id-of league1) (id-of season1)))

;;in progress.
;;Take a collection of collections and a value
;;Breadth first search: check if the value is in
;;the topmost collection.
;;If not, recursively call find-in on each nested collection.
;;Return collection containing the value or nil if not found.
#_(defn find-in [coll key] ;;not working
  (when (first coll)
    (cond (some #(= key (first %)) coll) coll
          (find-in (next coll) key) (next coll)
          (find-in (first coll) key) (first coll))))

(defn import-sports []
  (create-unique-index "sport_idx")
  (create-unique-index "sport_id_idx")
  (let [sports (kc/select sql-sport)]
    (doseq [sport sports]
      (let [{:keys [name slug sport_id]} sport
            sport-node (nn/create-unique-in-index
                        "sport_idx" :slug slug
                        {:name name
                         :slug slug
                         :node_type "sport"})]
        (nn/add-to-index sport-node "sport_id_idx" :id sport_id
                         {:unique true})))))

(defn import-score-types []
  (create-unique-index "score_type_idx")
  (create-unique-index "score_type_id_idx")
  (let [score-types (kc/select sql-score-type)]
    (doseq [score-type score-types]
      (let [{:keys [sport_id score_type_id
                    score_type_name score_type_point_value]} score-type
            sport-node (get-by-sql-id "sport_id_idx" sport_id)
            score-type-node (nn/create-unique-in-index
                             "score_type_idx" :name score_type_name
                             {:name        score_type_name
                              :point_value score_type_point_value
                              :node_type  "score_type"})]
        (nrl/create sport-node score-type-node :HAS_SCORE_TYPE)
        (nn/add-to-index score-type-node "score_type_id_idx"
                         :id score_type_id
                         {:unique true})))))

(defn import-action-types []
  (create-unique-index "action_type_idx")
  (create-unique-index "action_type_id_idx")
  (let [action-types (kc/select sql-action-type)]
    (doseq [action-type action-types]
      (let [{:keys [float_name action_type_id parent_id sport_id
                    single_team_action action_name player1_name
                    player2_name]} action-type
            player2_name (or player2_name "NA")
            float_name   (or float_name "")
            sport-node (get-by-sql-id "sport_id_idx" sport_id)
            action-type-node (nn/create-unique-in-index
                              "action_type_idx" :name action_name    
                              {:float_name         float_name
                               :single_team_action single_team_action
                               :name               action_name
                               :player1_name       player1_name
                               :player2_name       player2_name
                               :node_type         "action_type"})]
        (nn/add-to-index action-type-node "action_type_id_idx"
                         :id action_type_id {:unique true})
        (nrl/create sport-node action-type-node :HAS_ACTION_TYPE)
        (if (not= action_type_id parent_id)
          (nrl/create action-type-node
                      (get-by-sql-id "action_type_id_idx" parent_id)
                      :HAS_PARENT_TYPE))))))

(defn import-stat-types []
  (create-unique-index "stat_type_idx")
  (create-unique-index "stat_type_id_idx")
  (let [stat-types (kc/select sql-stat-type)]
    (doseq [stat-type stat-types]
      (let [{:keys [stat_type_id sport_id  stat_name
                    int1_name    int2_name int3_name
                    int4_name]} stat-type
            sport-node (get-by-sql-id "sport_id_idx" sport_id)
            int2_name  (or int2_name "")
            int3_name  (or int3_name "")
            int4_name  (or int4_name "")
            stat-type-node (nn/create-unique-in-index
                            "stat_type_idx" :name stat_name
                            {:name       stat_name
                             :int1_name  int1_name
                             :int2_name  int2_name
                             :int3_name  int3_name
                             :int4_name  int4_name
                             :node_type "stat_type"})]
        (nn/add-to-index stat-type-node "stat_type_id_idx"
                         :id stat_type_id {:unique true})
        (nrl/create sport-node stat-type-node :HAS_STAT_TYPE)))))

(defn import-positions []
  (create-unique-index "position_idx")
  (create-unique-index "position_id_idx")
  (let [positions (kc/select sql-position)]
    (doseq [position positions]
      (let [{:keys [position_id   sport_id
                    position_name position_abbr]} position
            sport-node (get-by-sql-id "sport_id_idx" sport_id)
            position-node (nn/create-unique-in-index
                           "position_idx" :name position_name
                           {:name       position_name
                            :abbr       position_abbr
                            :node_type "position"})]
        (nn/add-to-index position-node "position_id_idx"
                         :id position_id {:unique true})
        (nrl/create sport-node position-node "HAS_POSITION")))))

(defn import-coaches []
  (create-unique-index "coach_idx")
  (create-unique-index "coach_id_idx")
  (let [coaches-seq (kc/select sql-coach
                      (kc/fields :id :firstname :lastname :slug))]
    (doseq [coach coaches-seq]
      (let [{:keys [id firstname lastname slug]} coach
            coach-node (nn/create-unique-in-index
                        "coach_idx" :slug slug
                        {:first_name firstname
                         :last_name  lastname
                         :slug       slug
                         :node_type "coach"})]
        (nn/add-to-index coach-node "coach-id-idx"
                         :id id {:unique true})))))

;;~15 minutes to import all of them
(defn import-players []
  (create-unique-index "student_idx")
  (create-unique-index "player_id_idx")
  (let [players (kc/select sql-player
                  (kc/fields :player_id :slug :firstname :lastname))] 
    (doseq [player players]
      (let [{:keys [player_id slug first_name last_name]} player
            first_name (or first_name "")
            last_name  (or last_name  "")
            player-node (nn/create-unique-in-index
                         "student_idx" :slug slug
                         {:slug        slug
                          :first_name  first_name
                          :last_name   last_name
                          :node_type  "student"})] 
        (nn/add-to-index player-node "player_id_idx" :id player_id
                         {:unique true})))))

(defn import-schools []
  (create-unique-index "school_idx")
  (create-unique-index "school_id_idx")
  (let [schools (kc/select sql-school
                 (kc/join :left sql-team (= :school.school_id
                                            :team.school_id)) 
                 (kc/fields :slug :initials :state :name :address1
                            :address2 :city :phone :url :zip
                            :team.mascot :venue_only)
                 (kc/group :school_id))]
    (doseq [{:keys [slug initials state  zip url
                    name address1 phone  school_id  
                    city address2 mascot venue_only]} schools]
      (let [phone    (or phone "")
            address1 (or address1 "")
            address2 (or address2 "")
            zip      (or zip "")
            mascot   (or mascot "")
            initials (or initials "")  
            url      (str (re-find url-re (str url)))
            school-node (nn/create-unique-in-index
                         "school_idx" :slug slug
                         {:slug      slug     :initials   initials
                          :state     state    :name       name
                          :address1  address1 :address2   address2
                          :city      city     :phone      phone
                          :url       url      :zip        zip
                          :mascot    mascot   :venue_only venue_only
                          :node_type "school"})]
        (nn/add-to-index school-node "school_id_idx" :id school_id
                         {:unique true})))))

(defn import-seasons []
  (create-unique-index "season_idx")
  (create-unique-index "season_id_idx")
  (let [seasons (kc/select sql-season
                  (kc/fields :weeks :start_date :season_name
                             :sport_id :season_id))]
    (doseq [{:keys [weeks start_date season_name
                    sport_id season_id]} seasons]
      (let [season-node (nn/create-unique-in-index
                         "season_idx" :name season_name
                         {:weeks      weeks
                          :start_date start_date
                          :name       season_name
                          :node_type "season"})
            sport-node (get-by-sql-id "sport_id_idx" sport_id)]
        (nn/add-to-index season-node "season_id_idx" :id season_id
                         {:unique true})
        (nrl/create sport-node season-node :HAS_SEASON)))))

(defn import-leagues []
  (create-unique-index "league_idx")
  (create-unique-index "league_id_idx")
  (let [leagues (kc/select sql-league)]
    (doseq [{:keys [league_name type sport_id league_id]} leagues] 
      (let [league-node (nn/create-unique-in-index
                         "league_idx" :name league_name
                         {:name league_name
                          :type type
                          :node_type "league"})
            sport-node (get-by-sql-id "sport_id_idx" sport_id)]
        (nn/add-to-index league-node "league_id_idx" :id league_id
                         {:unique true})
        (nrl/create sport-node league-node :HAS_LEAGUE)))))

(defn import-teams []
  (create-unique-index "team_id_idx")
  (let [teams (kc/select sql-team
                (kc/fields :type :school_id :sport_id
                           :team_id :mascot))]
    (doseq [{:keys [type mascot sport_id school_id team_id]} teams]
      (let [mascot (or mascot "")
            team-node (nn/create-unique-in-index
                       "team_id_idx" :id team_id
                       {:type type :mascot mascot
                        :node_type "team"})
            sport-node (get-by-sql-id "sport_id_idx" sport_id)
            school-node (get-by-sql-id "school_id_idx" school_id)]
        (nrl/create team-node sport-node :COMPETES_IN)
        (nrl/create team-node school-node :REPRESENTS)))))

;;No index, will be found through the league nodes --done
(defn import-league-seasons [] ;;Section = node_id:119123
  (let [section-node (nn/create {:name "Central California"
                                 :node_type "section"}) 
        league-seasons (kc/select sql-league-season)]
    (doseq [{:keys [sequence  league_id 
                    season_id section_id]} league-seasons]
      (let [sequence (or sequence -1)
            league-season-node (nn/create {:sequence   sequence
                                           :node_type "league_season"})
            season-node (get-by-sql-id "season_id_idx" season_id)
            league-node (get-by-sql-id "league_id_idx" league_id)]
        (nrl/create league-node league-season-node :HAS_LEAGUE_SEASON)
        (nrl/create league-season-node season-node :DURING_SEASON)
        (nrl/create league-season-node section-node :IN_SECTION)))))

(defn import-divisions [] ;;done
  (create-unique-index "division_id_idx")
  (loop [division-number 6]
    (if (>= division-number 0)
      (do (nn/create-unique-in-index
           "division_id_idx" :id division-number
           {:division division-number :node_type "division"})
          (recur (dec division-number))))))

;;No index, reached via school->team->team_season
(defn import-team-seasons [] ;;done - 84seconds
  (let [team-seasons (kc/select sql-team-season
                       #_(kc/where (= :team_id 63)))]
    (doseq [{:keys [team_id season_id
                    league_id division]} team-seasons]
      (let [division (or division 0)
            team-season-node (nn/create {:node_type "team_season"})
            team-node (get-by-sql-id "team_id_idx" team_id)
            division-node (get-by-sql-id "division_id_idx" division)
            season-node (get-by-sql-id "season_id_idx" season_id)
            league-node (get-by-sql-id "league_id_idx" league_id)
            league-season-node (try (nn/get
                                      (id-of (get-node-betwixt
                                              (id-of season-node)
                                              (id-of league-node)
                                              "DURING_SEASON"
                                              "HAS_LEAGUE_SEASON"))) 
                                    (catch Exception e 0))]
        (when (not= 0 league-season-node)
          (nrl/create team-node team-season-node
                      :HAD_SEASON)
          (nrl/create team-season-node season-node
                      :COMPETED_IN_SEASON)
          (nrl/create team-season-node league-season-node
                      :COMPETED_IN_LEAGUE)
          (nrl/create team-season-node division-node
                      :COMPETED_IN_DIVISION))))))

(defn import-games [] ;;done - 272 seconds
  (create-unique-index "game_id_idx")
  (let [games (kc/exec-raw "select game_id, season_id, sport_id, game_date, school_id, status, summary,editor_notes, team1_id, team1host, team2_id, team2host, game_type_name from game natural join (select game_id, m1.team_id as team1_id, m1.host as team1host,m2.team_id as team2_id, m2.host as team2host from matchup as m1 join matchup as m2 using(game_id) where m1.team_id != m2.team_id group by game_id) as subtable1 left join game_type using(game_type_id) where game_date > 0;" :results)]
    (doseq [{:keys [game_id sport_id season_id team1_host
                    summary team1_id school_id team2_host
                    status  team2_id game_date editor_notes
                    game_type_name]} games]
      (let [summary (or summary "")
            editor_notes (or editor_notes "")
            team1_host (or team1_host false)
            team2_host (or team2_host false) 
            game-node (nn/create-unique-in-index
                       "game_id_idx" :game_id game_id
                       {:summary          summary 
                        :status           status  
                        :team1_host?      team1_host
                        :team2_host?      team2_host
                        :competition_date game_date
                        :editor_notes     editor_notes
                        :competition_type game_type_name
                        :node_type       "competition"})
            season-id (id-of (get-by-sql-id "season_id_idx" season_id))
            team1-id (id-of (get-by-sql-id "team_id_idx" team1_id))
            team2-id (id-of (get-by-sql-id "team_id_idx" team2_id))
            team1-season-id (try (id-of (get-node-betwixt
                                         (team1-id) (season-id)
                                         "HAD_SEASON"
                                         "COMPETED_IN_SEASON")) 
                                 (catch Exception e 0))
            team2-season-id (try (id-of (get-node-betwixt
                                         (team2-id) (season-id)
                                         "HAD_SEASON"
                                         "COMPETED_IN_SEASON")) 
                                 (catch Exception e 0))]
        (nrl/create season-id game-node :HAD_MATCHUP)
        (when (not= team1-season-id 0) 
          (nrl/create team1-season-id game-node :COMPETED_IN
                      {:host? team1_host}))
        (when (not= team2-season-id 0) 
          (nrl/create team2-season-id game-node :COMPETED_IN
                      {:host? team2_host}))))))

(defn import-scores []
  (let [scores (kc/select sql-score)]
    (doseq [{:keys [score period team_id game_id]} scores]
      (let [team-id (try (id-of (get-by-sql-id "team_id_idx" team_id))
                         (catch Exception e 0))
            game-id (try (id-of (get-by-sql-id "game_id_idx" game_id))
                         (catch Exception e 0))
            team-season-id (try (id-of (get-node-betwixt
                                         (team-id) (game-id)
                                         "HAD_SEASON"
                                         "COMPETED_IN")) 
                                 (catch Exception e 0))]
        (when (not= team-season-id 0)
          (nrl/create team-season-id game-id
                      :SCORED_IN {:score score :period period}))))))

#_(defn import-stats []
  (let [stats (kc/select sql-stat
                (kc/join :left [sql-stat-type :st]
                         (= :stat.stat_type_id :st.stat_type_id))
                (kc/fields :stat_id :game_id :team_id :player1_id
                           :i1 :i2 :i3 :i4 :st.stat_name :st.int1_name
                           :st.int2_name :st.int3_name :st.int4_name)
                #_(kc/limit 12000) #_(kc/offset 36000))]
    (doseq [{:keys [stat_id game_id team_id player1_id i1 i2 i3 i4
                    stat_name int1_name int2_name int3_name int4_name]}
            stats]
      (let [i1 (or i1 0)
            i2 (or i2 0)
            i3 (or i3 0)
            i4 (or i4 0)
            int3_name (or int3_name "")
            int4_name (or int4_name "")
            stat_node (nn/create {:stat_id stat_id :game_id game_id
                                  :team_id team_id :player1_id player1_id
                                  :i1 i1 :i2 i2 :i3 i3 :i4 i4
                                  :int1_name int1_name :int2_name int2_name
                                  :int3_name int3_name :int4_name int4_name})
            team_node (nn/find-one "team_idx" "team_id" team_id)
            game_node (nn/find-one "competition_idx" "game_id" game_id)
            player_node (nn/find-one "student_idx" "player_id" player1_id)]
        (and game_node (nrl/create game_node stat_node :STAT_IN))
        (and team_node (nrl/create team_node stat_node :HAS_STAT))
        (and player_node (nrl/create player_node stat_node :HAS_STAT))))))

#_(defn import-actions []
  (create-unique-index "action_idx")
  (let
      [actions (kc/exec-raw
        "select id, parent AS parent_id, game_id, team_id, player1_id,
player1_name,player2_id, player2_name, period, value, failed, notes,
score_type_name,score_type_point_value, action_type.action_type_id,
action_type.parent_id AS action_type_parent_id, single_team_action,
action_name, float_name FROM action LEFT JOIN (score_type, action_type)
ON (action.score_type_id = score_type.score_type_id
AND action.action_type_id = action_type.action_type_id)
limit 20000, 30000;" :results)]
    (doseq [{:keys [id parent_id action_name game_id
                    team_id score_type_point_value
                    score_type_name value float_name
                    failed player1_id player1_name
                    player2_id player2_name period
                    notes single_team_action]} actions]
      (let [parent_id (or parent_id -1)
            player1_id (or player1_id -1)
            player2_id (or player2_id -1)
            player2_name (or player2_name "")
            period (or period 0)
            value (or value 0.0)
            failed (or failed 0)
            notes (or notes "")
            float_name (or float_name "")
            action_node (nn/create-unique-in-index
                         "action_idx" :action_id id
                         {:action_id id :parent_id parent_id
                          :action_name action_name :game_id game_id
                          :team_id team_id
                          :point_value score_type_point_value
                          :score_type score_type_name :value value
                          :value_unit float_name :failed failed
                          :player1_id player1_id :player1_name player1_name
                          :player2_id player2_id :player2_name player2_name
                          :period period :notes notes
                          :single_team_action single_team_action})
            ;; 5 relationships self, team, game, p1, p2
            team_node (nn/find-one "team_idx" "team_id" team_id)
            game_node (nn/find-one "competition_idx" "game_id" game_id)
            player1_node (nn/find-one "student_idx" "player_id" player1_id)
            player2_node (nn/find-one "student_idx" "player_id" player2_id)
            action_node_p (nn/find-one "action_idx" "action_id" parent_id)]
        (and game_node (nrl/create game_node action_node :HAD_ACTION))
        (and team_node (nrl/create team_node action_node :HAD_ACTION))
        (and player1_node (nrl/create player1_node action_node :INITIATED_ACTION))
        (and player2_node (nrl/create player2_node action_node :COMPLETED_ACTION))
        (and action_node_p (nrl/create action_node_p action_node :PARENT_ACTION))
        )))) 

#_(defn import-coach-seasons []
  (let [coach-seasons (kc/select sql-coach-team-season
                        (kc/fields :coach_id, :team_id, :season_id,
                                   :type))]
    (doseq [{:keys [coach_id team_id season_id type]} coach-seasons]
      (let [coach_node (nn/find-one "coach_idx" "id" coach_id)
            team_node (nn/find-one "team_idx" "team_id" team_id)
            season_node (nn/find-one "season_idx" "season_id" season_id)]
        (and coach_node (nrl/create coach_node season_node :COACHED_IN_SEASON
                                    {:type type}))
        (and team_node (nrl/create coach_node team_node :COACHED_TEAM_IN
                                   {:season_id season_id :type type}))))))

#_(defn import-player-team-seasons "TODO" []
  (create))

#_(defn import-player-team-and-positions []
  (let [player-positions (kc/exec-raw "select player_team_season_position.id, position_id, player_id, team_id,season_id, jersey_number from player_team_season_position left join player_team_season on player_team_season_position.player_team_season_id = player_team_season.id;" :results)]
    (doseq [{:keys [id position_id player_id team_id
                    season_id jersey_number]} player-positions]
      (let [jersey_number (or jersey_number -1)
            player_node (nn/find-one "student_idx" "player_id" player_id)
            position_node (nn/find-one "position_idx" "position_id" position_id)
            team_node (nn/find-one "team_idx" "team_id" team_id)
            season_node (nn/find-one "season_idx" "season_id" season_id)]
        (do (and (and player_node position_node)
                 (nrl/create player_node position_node "PLAYS_POSITION"
                             {:season_id season_id
                              :team_id team_id}))
            (and (and player_node team_node)
                 (nrl/create player_node team_node "PLAYS_FOR_TEAM"
                             {:season_id season_id
                              :position_id position_id
                              :jersey_number jersey_number}))
            (and (and player_node team_node)
                 (nrl/create team_node player_node "HAS_PLAYER"
                             {:season_id season_id
                              :position_id position_id
                              :jersey_number jersey_number}))
            (and (and player_node season_node)
                 (nrl/create player_node season_node "PLAYED_IN_SEASON"
                             {:position_id position_id
                              :team_id team_id})))))))

#_(defn relate-teams-to-schools []
  (let [team-ids (query "team_id" {:index "team_idx"
                                   :return "node_id"
                                   :limit 999})]
    (doseq [team-id team-ids]
      (let [team (trim-node (nn/get team-id))
            school (trim-node (nn/find-one "school_idx" "school_id"
                                           (:school_id team)))]
        (nrl/create team-id (:node_id school) :REPRESENTS)))))

#_(defn relate-teams-to-games []
  (let [game-node-ids (query "game_id" {:index "competition_idx"
                                   :return "node_id"
                                   :limit 4000})]
    (doseq [game-node-id game-node-ids]
      (let [game (trim-node (nn/get game-node-id))
            team1 (trim-node (nn/find-one "team_idx" "team_id"
                                          (:team1_id game)))
            team2 (trim-node (nn/find-one "team_idx" "team_id"
                                          (:team2_id game)))]
        (do (and team1
                 (nrl/create (nn/get (:node_id team1)) (nn/get game-node-id) 
                             "COMPETED_IN" {:host? (:team1_host? game)}))
            (and team2
                 (nrl/create (nn/get (:node_id team2)) (nn/get game-node-id) 
                             "COMPETED_IN" {:host? (:team2_host? game)})))))))

;;rerun this now that team is being used
#_(defn relate-players-to-schools []
  (let [player-node-ids (query "player_id" {:index "student_idx"
                                            :return "node_id"
                                            :limit 40000})]
    (doseq [player-node-id player-node-ids]
      (let [player (trim-node (nn/get player-node-id))
            team (trim-node (nn/find-one "team_idx" "team_id"
                                         (:team_id player)))
            school (trim-node (nn/find-one "school_idx" "school_id"
                                           (:school_id team)))]
        (and school
             (nrl/create player-node-id (:node_id school)
                         "ATTENDS"))))))

(defn relate-action-type-score-types
  "TODO" [])





















"current node property types"
#_(:weeks :team2_id :status :period :league_id :notes :slug :stat_name :player2_id :initials :school_id :state :int3_name :team1_id :sport_id :score_type :player1_name :testkey1 :score_type_id :action_id :jersey_number :testkey2 :value_unit :weight :position_name :team1_host? :name :single_team_action :action_type_id :score_type_point_value :float_name :int1_name :game_id :game_date :stat_id :address1 :game_type :team_id :season_name :failed :address2 :int4_name :i4 :city :start_date :pts_id :i3 :i2 :score_type_name :phone :point_value :url :stat_type_id :editor_notes :i1 :firstname :type :parent_id :position_abbr :player_id :lastname :zip :season_id :player1_id :team2_host? :action_name :mascot :summary :id :position_id :value :league_name :venue_only :height :int2_name :player2_name)
#_(select id, parent AS parent_id, game_id, team_id, player1_id, player1_name,
    player2_id, player2_name, period, value, failed, notes, score_type_name,
    score_type_point_value, action_type.action_type_id, action_type.parent_id
    AS action_type_parent_id, single_team_action, action_name, float_name
    FROM action
    LEFT JOIN (score_type, action_type)
         ON (action.score_type_id = score_type.score_type_id
             AND action.action_type_id = action_type.action_type_id))
#_(kc/defentity sql-sport done
(kc/defentity sql-position) done
(kc/defentity sql-coach imported
(kc/defentity sql-player imported
(kc/defentity sql-school done
(kc/defentity sql-season imported
(kc/defentity sql-team imported
(kc/defentity sql-league imported 
(kc/defentity sql-player-team-season done
(kc/defentity sql-game done
(kc/defentity sql-game-type done
(kc/defentity sql-matchup done
(kc/defentity sql-score done (as relations)
(kc/defentity sql-stat imported, done
(kc/defentity sql-stat-type imported, done 
(kc/defentity sql-score-type imported
(kc/defentity sql-action imported
(kc/defentity sql-action-type imported
(kc/defentity sql-action-type-score-type imported
(kc/defentity sql-coach-team-season
(kc/defentity sql-player-team-season-position
(kc/defentity sql-league-season
(kc/defentity sql-team-season

    ))))))))))))))))))))))
#_((kc/defentity sql-sport)
  (kc/defentity sql-coach
    (kc/has-many sql-coach-team-season))
  (kc/defentity sql-game-type
    (kc/has-many sql-game))
  (kc/defentity sql-player
    (kc/has-many sql-stat))
  (kc/defentity sql-school
    (kc/has-many sql-game)
    (kc/has-one sql-team))
  (kc/defentity sql-score-type
    (kc/has-many sql-action)
    (kc/has-many sql-action-type-score-type))
  (kc/defentity sql-season
    (kc/belongs-to sql-sport)
    (kc/has-many sql-coach-team-season)
    (kc/has-many sql-game)
    (kc/has-many sql-league-season)
    (kc/has-many sql-team-season))
  (kc/defentity sql-stat-type
    (kc/belongs-to sql-sport)
    (kc/has-many sql-stat))
  (kc/defentity sql-team
    (kc/belongs-to sql-school)
    (kc/belongs-to sql-sport)
    (kc/has-many sql-action)
    (kc/has-many sql-coach-team-season)
    (kc/has-many sql-matchup)
    (kc/has-many sql-score)
    (kc/has-many sql-stat)
    (kc/has-many sql-team-season))
  (kc/defentity sql-game
    (kc/belongs-to sql-season)
    (kc/belongs-to sql-sport)
    (kc/belongs-to sql-game-type)
    (kc/belongs-to sql-school)
    (kc/has-many sql-action)
    (kc/has-many sql-score)
    (kc/has-many sql-stat)
    (kc/has-one sql-matchup))
  (kc/defentity sql-action-type
    (kc/belongs-to sql-sport)
    (kc/has-many sql-action)
    (kc/has-many sql-action-type-score-type))
  (kc/defentity sql-action
    (kc/belongs-to sql-game)
    (kc/belongs-to sql-team)
    (kc/belongs-to sql-action-type)
    (kc/belongs-to sql-score-type))
  (kc/defentity sql-action-type-score-type
    (kc/belongs-to sql-action-type)
    (kc/belongs-to sql-score-type))
  (kc/defentity sql-coach-team-season
    (kc/belongs-to sql-coach)
    (kc/belongs-to sql-team))
  (kc/defentity sql-league
    (kc/belongs-to sql-sport)
    (kc/has-many sql-league-season)
    (kc/has-many sql-team-season))
  (kc/defentity sql-league-season
    (kc/belongs-to sql-league)
    (kc/belongs-to sql-season))
  (kc/defentity sql-matchup
    (kc/belongs-to sql-game)
    (kc/belongs-to sql-team))
  (kc/defentity sql-player-team-season
    (kc/belongs-to sql-player)
    (kc/belongs-to sql-team)
    (kc/belongs-to sql-season))
  (kc/defentity sql-player-team-season-position
    (kc/belongs-to sql-player-team-season)
    (kc/belongs-to sql-position))
  (kc/defentity sql-score
    (kc/belongs-to sql-game)
    (kc/belongs-to sql-team))
  (kc/defentity sql-stat
    (kc/belongs-to sql-stat-type)
    (kc/belongs-to sql-game)
    (kc/belongs-to sql-player)
    (kc/belongs-to sql-team))
  (kc/defentity sql-team-season
    (kc/belongs-to sql-team)
    (kc/belongs-to sql-season)
    (kc/belongs-to sql-league)))

