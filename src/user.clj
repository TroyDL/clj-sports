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



(def index-map {"sports_idx" "sport_id"
                "positions_idx" "position_id"
                "coaches_idx" "id"
                "players_idx" "player_id"
                "schools_idx" "school_id"
                "seasons_idx" "season_id"
                "leagues_idx" "league_id"
                "teams_idx" "team_id"
                "games_idx" "game_id"
                "actions_idx" "action_id"})

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
  "Helper for query function.  Compiles query string."
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
                           #"node/|relationship/")))))

(defn trim-node
  "Trims typically unwanted data from a get, query or tquery response of
   neocons.  Does not work on relationships; see trim-rel."
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
          (id-of node))))

(defn trim-rel
  "TODO"
  [rel]
  (cond (= (type rel) clojurewerkz.neocons.rest.records.Relationship)
        "todo"
        (= (type rel) clojurewerkz.neocons.rest.records.CypherQueryResponse)
        "todo"
        (= (type rel) clojure.lang.LazySeq)
        "todo"))

(defn delete-nodes [nodes]
  (nn/delete (nn/get (first nodes)))
  (if (first nodes)
    (delete-nodes (next nodes))))

(defn destroy-nodes [nodes]
  (nn/destroy (nn/get (first nodes)))
  (if (first nodes)
    (destroy-nodes (next nodes))))

(defn query
  "This is a general purpose Neo4J query function.
   Returns all nodes in a graph that have the given
   property.  Map options are :index, :val, :limit
   :return.  Restrict to an index or property value
   and return a list of a particular property.
   Limit default is 10.  Result contains count meta."
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
                  (conj res
                        (assoc (:data curr) :node_id
                               (Integer.
                                (second
                                 (str/split (:self curr) #"node/")))))
                  (inc node-count)) 
           (if count?
             (println node-count)
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

(defn rel-query [rel-property limit]
  ())

(defn count-index [index]
  (query (index-map index) {:index index :count true}))

(defn find-all-rel-properties-helper
  "Returns a set of all properties from all nodes in a
   Neo4J graph via NeoCons library."
  [skip]
  (loop [rels (:data
                (cy/query
                 (str "start r=rel(*) return r skip " skip " limit 1000")))
             properties ()]
           (if-let [rel (:data (first (first rels)))]
                   (recur (rest rels)
                          (seq (set (flatten (conj properties (keys rel))))))
                   properties)))

(defn find-all-rel-properties []
  (let [rel-count
        (first (first (:data (cy/query "start r=rel(*) return count(r)"))))] 
    (loop [rel-set ()
           skip 0]
      (if (< skip rel-count)
        (recur (seq (set (flatten (conj rel-set
                                        (find-all-rel-properties-helper skip)))))
               (+ skip 1000))
        rel-set))))

(defn find-all-properties-helper
  "Returns a set of all properties from all nodes in a
   Neo4J graph via NeoCons library."
  [skip]
  (loop [nodes (:data
                (cy/query
                 (str "start n=node(*) return n skip " skip " limit 1000")))
             properties ()]
           (if-let [node (:data (first (first nodes)))]
                   (recur (rest nodes)
                          (seq (set (flatten (conj properties (keys node))))))
                   properties)))

(defn find-all-properties []
  (let [node-count
        (first (first (:data (cy/query "start n=node(*) return count(n)"))))] 
    (loop [node-set ()
           skip 0]
      (if (< skip node-count)
        (recur (seq (set (flatten (conj node-set
                                        (find-all-properties-helper skip)))))
               (+ skip 1000))
        node-set))))

(defn create-unique-index [name]
  (try (nn/create-index name {:unique true})
       (catch Exception e)))

(defn import-sports []
  (try (nn/create-index "sports_idx"
                           {:unique true})
          (catch Exception e 
            "borked, probably already exists"))
  (let [sports-seq (kc/select sql-sport)]
    (doseq [sport sports-seq]
      (nn/create-unique-in-index "sports_idx"
                                 :sport_id
                                 (:sport_id sport)
                                 sport))))

(defn import-score-types []
  (let [score-types (kc/select sql-score-type)]
    (doseq [score-type score-types]
      (let [sport-node
            (nn/find-one "sports_idx" "sport_id" (:sport_id score-type))
            score-type-node (nn/create score-type)]
        (nrl/create sport-node score-type-node :HAS_SCORE_TYPE)))))

(defn import-action-types []
  (let [action-types (kc/select sql-action-type)]
    (doseq [action-type action-types]
      (let [{:keys [float_name action_type_id parent_id sport_id
                    single_team_action action_name player1_name
                    player2_name]} action-type
            sport-node (nn/find-one
                        "sports_idx" "sport_id" sport_id)
            player2_name (or player2_name "NA")
            float_name (or float_name "")
            action-type-node (nn/create
                              {:float_name float_name
                               :action_type_id action_type_id
                               :parent_id parent_id
                               :sport_id sport_id
                               :single_team_action single_team_action
                               :action_name action_name
                               :player1_name player1_name
                               :player2_name player2_name})]
        (nrl/create sport-node action-type-node :HAS_ACTION_TYPE)))))

(defn import-stat-types []
  (let [stat-types (kc/select sql-stat-type)]
    (doseq [stat-type stat-types]
      (let [sport-node
            (nn/find-one "sports_idx" "sport_id" (:sport_id stat-type))
            {:keys [stat_type_id sport_id stat_name
                    int1_name int2_name int3_name
                    int4_name]} stat-type
            int2_name (or int2_name "")
            int3_name (or int3_name "")
            int4_name (or int4_name "")
            stat-type-node (nn/create {:stat_type_id stat_type_id
                                       :sport_id sport_id
                                       :stat_name stat_name
                                       :int1_name int1_name
                                       :int2_name int2_name
                                       :int3_name int3_name
                                       :int4_name int4_name})]
        (nrl/create sport-node stat-type-node :HAS_STAT_TYPE)))))

(defn import-positions []
  (try (nn/create-index "positions_idx"
                        {:unique true})
       (catch Exception e "exists already"))
  (let [positions (kc/select sql-position)]
    (doseq [position positions]
      (let [{:keys [position_id sport_id
                    position_name position_abbr]} position
            sport_node (nn/find-one "sports_idx" "sport_id" sport_id)
            position_node (nn/create-unique-in-index "positions_idx"
                           :position_id position_id
                           position)]
        (nrl/create sport_node position_node "HAS_POSITION")))))

(defn import-coaches []
  (create-unique-index "coaches_idx")
  (let [coaches-seq (kc/select sql-coach
                      (kc/fields :id :firstname :lastname :slug))]
    (doseq [coach coaches-seq]
      (nn/create-unique-in-index "coaches_idx"
                                 :id
                                 (:id coach)
                                 coach))))

(defn kill-player-nil [node]
  (-> (update-in node [:season_id] str)
      (update-in [:height] str)
      (update-in [:weight] str)
      (update-in [:jersey_number] str)
      (update-in [:team_id] str)
      (update-in [:pts_id] str)
      (update-in [:firstname] (comp str/trim str))))

(defn import-players []
    (create-unique-index "players_idx")
    (let [players-seq (kc/select sql-player-team-season
                        (kc/fields :season_id :height :weight
                                   :jersey_number :team_id
                                   [:id :pts_id])
                        (kc/with sql-player 
                                 (kc/fields :player_id :slug
                                            :firstname :lastname)))]
      (doseq [player players-seq]
        (let [player' (kill-player-nil player)] 
          (nn/create-unique-in-index "players_idx"
                                     :player_id (:player_id player')
                                     player')))))

(defn import-schools []
  (create-unique-index "schools_idx")
  (let [schools (kc/select sql-team
                  (kc/fields :school_id :mascot)
                  (kc/with sql-school))]
    (doseq [{:keys [slug initials school_id state
                    name address1 address2 city
                    phone url zip mascot venue_only]} schools]
      (let [phone (or phone "")
            address1 (or address1 "")
            address2 (or address2 "")
            url (or url "")
            zip (or zip "")
            mascot (or mascot "")
            initials (str initials)] 
        (pprint school_id) 
        (nn/create-unique-in-index "schools_idx"
                                   :school_id school_id
                                   {:slug slug :initials initials
                                    :school_id school_id :state state
                                    :name name :address1 address1
                                    :address2 address2 :city city
                                    :phone phone :url url :zip zip
                                    :mascot mascot
                                    :venue_only venue_only})))))

(defn import-seasons []
  (create-unique-index "seasons_idx")
  (let [schools (kc/select sql-season
                  (kc/fields :weeks :start_date :season_name
                             :sport_id :season_id))]
    (doseq [school schools]
      (nn/create-unique-in-index "seasons_idx"
                                 :season_id (:season_id school)
                                 school))))

(defn import-leagues []
  (create-unique-index "leagues_idx")
  (let [leagues (kc/select sql-league)]
    (doseq [league leagues] 
      (nn/create-unique-in-index "leagues_idx"
                                 :league_id (:league_id league)
                                 league))))

(defn import-teams []
  (create-unique-index "teams_idx")
  (let [teams (kc/select sql-team
                (kc/fields :type :school_id :sport_id :team_id))]
    (doseq [team teams]
      (nn/create-unique-in-index "teams_idx"
                                 :team_id (:team_id team)
                                 team))))

(defn import-games []
  (create-unique-index "games_idx")
  (let [games (kc/exec-raw "select game_id, season_id, sport_id, game_date, school_id, status, summary,editor_notes, team1_id, team1host, team2_id, team2host, game_type_name from game natural join (select game_id, m1.team_id as team1_id, m1.host as team1host,m2.team_id as team2_id, m2.host as team2host from matchup as m1 join matchup as m2 using(game_id) where m1.team_id != m2.team_id group by game_id) as subtable1 left join game_type using(game_type_id) where game_date > 0;" :results)]
    (doseq [{:keys [game_id season_id sport_id school_id status
                    game_date summary editor_notes team1_id
                    team1_host team2_id team2_host
                    game_type_name]} games]
      (let [summary (or summary "")
            editor_notes (or editor_notes "")
            team1_host (or team1_host false)
            team2_host (or team2_host false)]
        (nn/create-unique-in-index "games_idx"
                                   :game_id game_id
                                   {:game_id game_id
                                    :season_id season_id
                                    :sport_id sport_id
                                    :school_id school_id
                                    :status status
                                    :game_date game_date
                                    :summary summary
                                    :editor_notes editor_notes
                                    :team1_id team1_id
                                    :team1_host? team1_host
                                    :team2_id team2_id
                                    :team2_host? team2_host
                                    :game_type game_type_name})))))


(defn import-scores []
  (let [scores (kc/select sql-score)]
    (doseq [{:keys [score period team_id game_id]} scores]
      (let [team_node (nn/find-one "teams_idx" "team_id" team_id)
            game_node (nn/find-one "games_idx" "game_id" game_id)]
        (nrl/create team_node game_node :SCORED_IN {:score score
                                                    :period period})))))

(defn import-stats []
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
            team_node (nn/find-one "teams_idx" "team_id" team_id)
            game_node (nn/find-one "games_idx" "game_id" game_id)
            player_node (nn/find-one "players_idx" "player_id" player1_id)]
        (and game_node (nrl/create game_node stat_node :STAT_IN))
        (and team_node (nrl/create team_node stat_node :HAS_STAT))
        (and player_node (nrl/create player_node stat_node :HAS_STAT))))))

(defn import-actions []
  (create-unique-index "actions_idx")
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
                         "actions_idx" :action_id id
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
            team_node (nn/find-one "teams_idx" "team_id" team_id)
            game_node (nn/find-one "games_idx" "game_id" game_id)
            player1_node (nn/find-one "players_idx" "player_id" player1_id)
            player2_node (nn/find-one "players_idx" "player_id" player2_id)
            action_node_p (nn/find-one "actions_idx" "action_id" parent_id)]
        (and game_node (nrl/create game_node action_node :HAD_ACTION))
        (and team_node (nrl/create team_node action_node :HAD_ACTION))
        (and player1_node (nrl/create player1_node action_node :INITIATED_ACTION))
        (and player2_node (nrl/create player2_node action_node :COMPLETED_ACTION))
        (and action_node_p (nrl/create action_node_p action_node :PARENT_ACTION))
        )))) 


(defn import-coach-seasons []
  (let [coach-seasons (kc/select sql-coach-team-season
                        (kc/fields :coach_id, :team_id, :season_id,
                                   :type))]
    (doseq [{:keys [coach_id team_id season_id type]} coach-seasons]
      (let [coach_node (nn/find-one "coaches_idx" "id" coach_id)
            team_node (nn/find-one "teams_idx" "team_id" team_id)
            season_node (nn/find-one "seasons_idx" "season_id" season_id)]
        (and coach_node (nrl/create coach_node season_node :COACHED_IN_SEASON
                                    {:type type}))
        (and team_node (nrl/create coach_node team_node :COACHED_TEAM_IN
                                   {:season_id season_id :type type}))))))


(defn import-team-seasons []
  (let [team-seasons (kc/select sql-team-season)]
    (doseq [{:keys [team_id season_id league_id division]} team-seasons]
      (let [team_node (nn/find-one "teams_idx" "team_id" team_id)
            season_node (nn/find-one "seasons_idx" "season_id" season_id)
            league_node (nn/find-one "leagues_idx" "league_id" league_id)
            division (or division 0)]
        (when team_node
          (do (nrl/create team_node season_node :COMPETED_IN_SEASON
                          {:league_id league_id :division division})
              (and league_node
                   (nrl/create team_node league_node :COMPETED_IN_LEAGUE
                               {:division division
                                :season_id season_id}))))))))

(defn import-league-seasons []
  (let [league-seasons (kc/select sql-league-season)]
    (doseq [{:keys [sequence section_id season_id league_id]} league-seasons]
      (let [sequence (or sequence -1)
            season_node (nn/find-one "seasons_idx" "season_id" season_id)
            league_node (nn/find-one "leagues_idx" "league_id" league_id)]
        (and league_node
             (and season_node
                  (nrl/create league_node season_node :DURING_SEASON
                              {:sequence sequence
                               :section_id section_id})))))))

(defn import-player-team-and-positions []
  (let [player-positions (kc/exec-raw "select player_team_season_position.id, position_id, player_id, team_id,season_id, jersey_number from player_team_season_position left join player_team_season on player_team_season_position.player_team_season_id = player_team_season.id;" :results)]
    (doseq [{:keys [id position_id player_id team_id
                    season_id jersey_number]} player-positions]
      (let [jersey_number (or jersey_number -1)
            player_node (nn/find-one "players_idx" "player_id" player_id)
            position_node (nn/find-one "positions_idx" "position_id" position_id)
            team_node (nn/find-one "teams_idx" "team_id" team_id)
            season_node (nn/find-one "seasons_idx" "season_id" season_id)]
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

(defn relate-teams-to-schools []
  (let [team-ids (query "team_id" {:index "teams_idx"
                                   :return "node_id"
                                   :limit 999})]
    (doseq [team-id team-ids]
      (let [team (trim-node (nn/get team-id))
            school (trim-node (nn/find-one "schools_idx" "school_id"
                                           (:school_id team)))]
        (nrl/create team-id (:node_id school) :REPRESENTS)))))

(defn relate-teams-to-games []
  (let [game-node-ids (query "game_id" {:index "games_idx"
                                   :return "node_id"
                                   :limit 4000})]
    (doseq [game-node-id game-node-ids]
      (let [game (trim-node (nn/get game-node-id))
            team1 (trim-node (nn/find-one "teams_idx" "team_id"
                                          (:team1_id game)))
            team2 (trim-node (nn/find-one "teams_idx" "team_id"
                                          (:team2_id game)))]
        (do (and team1
                 (nrl/create (nn/get (:node_id team1)) (nn/get game-node-id) 
                             "COMPETED_IN" {:host? (:team1_host? game)}))
            (and team2
                 (nrl/create (nn/get (:node_id team2)) (nn/get game-node-id) 
                             "COMPETED_IN" {:host? (:team2_host? game)})))))))

;;rerun this now that team is being used
(defn relate-players-to-schools []
  (let [player-node-ids (query "player_id" {:index "players_idx"
                                            :return "node_id"
                                            :limit 40000})]
    (doseq [player-node-id player-node-ids]
      (let [player (trim-node (nn/get player-node-id))
            team (trim-node (nn/find-one "teams_idx" "team_id"
                                         (:team_id player)))
            school (trim-node (nn/find-one "schools_idx" "school_id"
                                           (:school_id team)))]
        (and school
             (nrl/create player-node-id (:node_id school)
                         "ATTENDS"))))))























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




;; create sql entity
;; create neo4j index
;; make fn for creating node on index
;; migrate sql entities to neo4j nodes

#_(defn upsert [index data & {:keys [index-key unique?]}]
  (let [lookup ((keyword index-key) data)
        node (or (nn/find-one (:name index) index-key lookup)
                 (nn/create data))]
    (nn/add-to-index (:id node) (:name index) index-key lookup
                     (boolean unique?))))

#_(defn create-season
  [{:keys [name weaks start_date]}]
  (upsert season-idx {:name name
                      :weeks weeks
                      :start_date start_date}
          :index-key "name"))

#_(defn query-prototype
  ([index-name]
     (pprint
      (map
       #(get-in % ["n" :data])
       (cy/tquery
        (str "start n = node:" (str index-name) "('"
             (index-map (str index-name)) ":*') return n limit 10")))))
  ([index-name return-key]
     (pprint
      (cy/query
       (str "start n = node:" (str index-name) "('"
            (index-map (str index-name)) ":*') return n." (str return-key)))))
  ([index-name index-value-like temp-arg]
     (pprint
      (map
       #(get-in % ["n" :data])
       (cy/tquery
        (str "start n = node:" (str index-name) "('"
             (index-map (str index-name)) ":*" (str index-value-like) "*')
        return n limit 10"))))))

#_(defn prototype1-text-query [key-name-str val-str]
  (pprint (map #(get-in % ["n" :data])
               (cy/tquery
                (str "start n=node(*) where HAS (n." key-name-str
                     ") AND n." key-name-str "=~ '(?i).*" val-str
                     ".*' return n limit 20")))))

#_(defn prototype2-text-query [key-name-str val-str]
  (cy/query
   (str "start n=node(*) where HAS (n." key-name-str
        ") AND n." key-name-str "=~ '(?i).*" val-str
        ".*' return n limit 2")))
