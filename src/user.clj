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



(def index-map {"sports_idx" "name"
                "positions_idx" "name"
                "coaches_idx" "id"
                "players_idx" "player_id"
                "schools_idx" "school_id"
                "seasons_idx" "season_id"
                "leagues_idx" "league_id"
                "teams_idx" "team_id"
                "games_idx" "game_id"})

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

(defn delete-nodes [nodes]
  (nn/delete (nn/get (first nodes)))
  (if (first nodes)
    (delete-nodes (rest nodes))))

(defn destroy-nodes [nodes]
  (nn/destroy (nn/get (first nodes)))
  (if (first nodes)
    (destroy-nodes (rest nodes))))

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

(defn find-all-properties
  "Returns a set of all properties from all nodes in a
   Neo4J graph via NeoCons library."
  []
  (loop [nodes (:data (cy/query "start n=node(*) return n"))
             node-set #{}]
           (if-let [node (:data (first (first nodes)))]
                   (recur (rest nodes)
                          (conj node-set (keys node)))
                   node-set)))


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
                                 :name
                                 (:name sport)
                                 sport))))

(defn import-positions []
  (try (nn/create-index "positions_idx"
                        {:unique true})
       (catch Exception e "exists already"))
  (let [positions-seq (kc/select sql-position)
        {sport-id :id} (first (nn/query "sports_idx" "name:Football"))]
    (doseq [pos positions-seq]
      (let [{pos-name :position_name} pos
            pos-id (get-in (nn/create-unique-in-index
                            "positions_idx"
                            :name pos-name pos)
                           [:id])]
        (do (nrl/create sport-id
                        pos-id 
                        "has_position"))))))

(defn import-coaches []
  (create-unique-index "coaches_idx")
  (let [coaches-seq (kc/select sql-coach
                      (kc/fields :id :firstname :lastname :slug))]
    (doseq [coach coaches-seq]
      (nn/create-unique-in-index "coaches_idx"
                                 :id
                                 (:id coach)
                                 coach))))

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

(defn kill-player-nil [node]
  (-> (update-in node [:season_id] str)
      (update-in [:height] str)
      (update-in [:weight] str)
      (update-in [:jersey_number] str)
      (update-in [:team_id] str)
      (update-in [:pts_id] str)
      (update-in [:firstname] (comp str/trim str))))

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
  (let [games (kc/exec-raw "select game_id, season_id, sport_id, game_date, school_id, status, summary,editor_notes, team1_id, team1host, team2_id, team2host, game_type_name from game natural join (select game_id, m1.team_id as team1_id, m1.host as team1host,m2.team_id as team2_id, m2.host as team2host from matchup as m1 join matchup as m2 using(game_id) where m1.team_id != m2.team_id group by game_id) as subtable1 left join game_type using(game_type_id) where game_date > 0 limit 4000;" :results)]
    (doseq [{:keys [game_id season_id sport_id school_id status
                    game_date summary editor_notes team1_id
                    team1_host team2_id team2_host
                    game_type_name]} games]
      (let [summary (or summary "")
            editor_notes (or editor_notes "")
            team1_host (or team1_host false)
            team2_host (or team2_host false)]
        (println game_id)
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
        (print (str game_id " "))
        (nrl/create team_node game_node :scored_in {:score score
                                                    :period period})))))

(defn import-stats []
  (let [stats (kc/select sql-stat
                (kc/join :left [sql-stat-type :st]
                         (= :stat.stat_type_id :st.stat_type_id))
                (kc/fields :stat_id :game_id :team_id :player1_id
                           :i1 :i2 :i3 :i4 :st.stat_name :st.int1_name
                           :st.int2_name :st.int3_name :st.int4_name))]
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
        (print ".")
        (and game_node (nrl/create game_node stat_node :stat_in))
        (and team_node (nrl/create team_node stat_node :has_stat))
        (and player_node (nrl/create player_node stat_node :has_stat))))))

#_(  select stat_id, game_id, team_id, player1_id, i1, i2, i3, i4, sport_id, stat_name, int1_name, int2_name, int3_name, int4_name
  from stat left join stat_type on stat.stat_type_id = stat_type.stat_type_id;
  )















#_(def game-query
  '(kc/select sql-game
     (kc/with sql-game-type
              (kc/fields :game_type_name))
     (kc/join [(kc/subselect sql-matchup
                             (kc/join [sql-matchup :m2]
                                      (= :m2.game_id :matchup.game_id))
                             (kc/where (not= :m2.team_id :matchup.team_id)) 
                             (kc/fields :game_id
                                        [:team_id :team1_id]
                                        [:m2.team_id :team2_id])
                             (kc/group :game_id)) :t2]
              (= :game.game_id :t2.game_id))
     (kc/fields :game_id :season_id
                :sport_id :school_id :status :game_date
                :summary :editor_notes :t2.team1_id
                :t2.team2_id :game_type.game_type_name)
     (kc/limit 2)))












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
(kc/defentity sql-score-type
(kc/defentity sql-action
(kc/defentity sql-action-type 
(kc/defentity sql-action-type-score-type
(kc/defentity sql-stat
(kc/defentity sql-stat-type ---
(kc/defentity sql-coach-team-season
(kc/defentity sql-player-team-season-position
(kc/defentity sql-league-season
(kc/defentity sql-team-season))))))))))))))))))))))

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
