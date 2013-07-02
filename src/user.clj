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
                "coaches_idx" "id"})
(defn query
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

(defn prototype1-text-query [key-name-str val-str]
  (pprint (map #(get-in % ["n" :data])
               (cy/tquery
                (str "start n=node(*) where HAS (n." key-name-str
                     ") AND n." key-name-str "=~ '(?i).*" val-str
                     ".*' return n limit 20")))))

(defn prototype2-text-query [key-name-str val-str]
  (cy/query
   (str "start n=node(*) where HAS (n." key-name-str
        ") AND n." key-name-str "=~ '(?i).*" val-str
        ".*' return n limit 2")))

(defn text-query
  ([key-name-str]
     (text-query key-name-str "" 10 "(*)"))

  ([key-name-str val-str]
     (text-query key-name-str val-str 10 "(*)"))

  ([key-name-str val-str limit-int]
     (text-query key-name-str val-str limit-int "(*)"))

  ([key-name-str val-str limit-int index-str]
     (let [index-str
           (if (not= index-str "(*)")
             (str \: index-str "('" (index-map index-str) ":*')")
             "(*)")]
       (loop [curr-seq (:data (cy/query
                               (format "start n=node%s where HAS (n.%s)
                         AND n.%s=~ '(?i).*%s.*' return n limit %d"
                                       index-str
                                       key-name-str key-name-str
                                       val-str limit-int))) res {}]
         (if-let [curr (first (first curr-seq))]
           (recur (rest curr-seq)
                  (assoc res (second (str/split (:self curr) #"node/"))
                         (:data curr)))
           (do (pprint res)
               (str (count res) " records found.")))))))


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



















#_(kc/defentity sql-sport
    sql-position
(kc/defentity sql-coach
(kc/defentity sql-game-type
(kc/defentity sql-player
(kc/defentity sql-school
(kc/defentity sql-score-type
(kc/defentity sql-season
(kc/defentity sql-stat-type ---
(kc/defentity sql-team
(kc/defentity sql-game
(kc/defentity sql-action-type
(kc/defentity sql-action
(kc/defentity sql-action-type-score-type
(kc/defentity sql-coach-team-season
(kc/defentity sql-league
(kc/defentity sql-league-season
(kc/defentity sql-matchup
(kc/defentity sql-player-team-season
(kc/defentity sql-player-team-season-position
(kc/defentity sql-score
(kc/defentity sql-stat
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
