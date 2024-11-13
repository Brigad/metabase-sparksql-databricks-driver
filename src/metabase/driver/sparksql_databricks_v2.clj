(ns metabase.driver.sparksql-databricks-v2
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [java-time.api :as t]
            [clojure
             [set :as set]
             [string :as str]]
            [honey.sql :as sql]
   									[honey.sql.helpers :as sql.helpers]
            [medley.core :as m]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]]
            [metabase.connection-pool :as pool]
            [metabase.driver :as driver]
            [metabase.driver.hive-like :as hive-like]
            [metabase.driver.sync :as driver.s]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.sql.parameters.substitution :as params.substitution]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql-jdbc.execute.legacy-impl :as sql-jdbc.legacy]
            [metabase.driver.sql.util :as sql.u]
            [metabase.legacy-mbql.util :as mbql.u]
            [metabase.query-processor.store :as qp.store]
            [metabase.query-processor.util :as qputil]
            [metabase.query-processor.util.add-alias-info :as add]
            [metabase.lib.metadata :as lib.metadata]
            [metabase.util.honey-sql-2 :as h2x]
            [metabase.util.log :as log]
            [metabase.util :as u])
  (:import [java.sql Connection ResultSet ResultSetMetaData Statement]
   [java.time LocalDate LocalDateTime LocalTime OffsetDateTime ZonedDateTime OffsetTime]))

(driver/register! :sparksql-databricks-v2, :parent :hive-like)

;;; ------------------------------------------ Custom HoneySQL Clause Impls ------------------------------------------


(def ^:private source-table-alias
  "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
  "t1")

;; ;; use `source-table-alias` for the source Table, e.g. `t1.field` instead of the normal `schema.table.field`
;; (defmethod sql.qp/->honeysql [:sparksql-databricks :field]
;;   [driver field]
;;   (binding [sql.qp/*table-alias* (or sql.qp/*table-alias* source-table-alias)]
;;     ((get-method sql.qp/->honeysql [:hive-like :field]) driver field)))

(defmethod sql-jdbc.sync/database-type->base-type :sparksql-databricks-v2
  [driver database-type]
  (condp re-matches (u/lower-case-en (name database-type))
    #"timestamp" :type/DateTimeWithLocalTZ
    #"timestamp_ntz" :type/DateTime
    ((get-method sql-jdbc.sync/database-type->base-type :hive-like)
     driver database-type)))

(defn- get-tables-sql
  [catalog]
  (assert (string? (not-empty catalog)))
  [(str/join
    "\n"
    ["select"
     "  TABLE_NAME as name,"
     "  TABLE_SCHEMA as schema,"
     "  COMMENT description"
     "  from system.information_schema.tables"
     "  where TABLE_CATALOG = ?"
     "    AND TABLE_SCHEMA <> 'information_schema'"])
   catalog])

(defn- describe-database-tables
  [database]
  (let [[inclusion-patterns
         exclusion-patterns] (driver.s/db-details->schema-filter-patterns database)
        syncable? (fn [schema]
                    (driver.s/include-schema? inclusion-patterns exclusion-patterns schema))]
    (eduction
     (filter (comp syncable? :schema))
     (sql-jdbc.execute/reducible-query database (get-tables-sql (-> database :details :catalog))))))

(defmethod driver/describe-database :sparksql-databricks-v2
  [driver database]
  (try
    {:tables (into #{} (describe-database-tables database))}
    (catch Throwable e
      (throw (ex-info (format "Error in %s describe-database: %s" driver (ex-message e))
                      {}
                      e)))))

(defmethod sql.qp/->honeysql [:sparksql-databricks-v2 :field]
  [driver [_ _ {::params.substitution/keys [compiling-field-filter?]} :as field-clause]]
  ;; use [[source-table-alias]] instead of the usual `schema.table` to qualify fields e.g. `t1.field` instead of the
  ;; normal `schema.table.field`
  (let [parent-method (get-method sql.qp/->honeysql [:hive-like :field])
        field-clause  (mbql.u/update-field-options field-clause
                                                   update
                                                   ::add/source-table
                                                   (fn [source-table]
                                                     (cond
                                                       ;; DO NOT qualify fields from field filters with `t1`, that won't
                                                       ;; work unless the user-written SQL query is doing the same
                                                       ;; thing.
                                                       compiling-field-filter? ::add/none
                                                       ;; for all other fields from the source table qualify them with
                                                       ;; `t1`
                                                       (integer? source-table) source-table-alias
                                                       ;; no changes for anyone else.
                                                       :else                   source-table)))]
    (parent-method driver field-clause)))

(defmethod sql.qp/apply-top-level-clause [:sparksql-databricks-v2 :page]
  [_driver _clause honeysql-form {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (sql.helpers/limit honeysql-form items)
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause [::over :%row_number (select-keys honeysql-form [:order-by])]]
        (-> (apply sql.helpers/select (map last (:select honeysql-form)))
            (sql.helpers/from (sql.helpers/select honeysql-form [over-clause :__rownum__]))
            (sql.helpers/where [:> :__rownum__ [:inline offset]])
            (sql.helpers/limit [:inline items]))))))

(defmethod sql.qp/apply-top-level-clause [:sparksql-databricks-v2 :source-table]
  [driver _ honeysql-form {source-table-id :source-table}]
  (let [{table-name :name, schema :schema} (lib.metadata/table (qp.store/metadata-provider) source-table-id)]
    (sql.helpers/from honeysql-form [(sql.qp/->honeysql driver (h2x/identifier :table schema table-name))
                                     [(sql.qp/->honeysql driver (h2x/identifier :table-alias source-table-alias))]])))


;;; ------------------------------------------- Other Driver Method Impls --------------------------------------------

;; (defmethod sql-jdbc.conn/connection-details->spec :sparksql-databricks [_ {:keys [host port db jdbc-flags], :as opts}]
;;   (merge
;;    {:classname                     "com.simba.spark.jdbc41.Driver" ; must be in classpath
;;     :subprotocol                   "spark"
;;     :subname                       (str "//" host ":" port "/" db jdbc-flags)
;;     :ssl                           true}
;;    (dissoc opts :host :port :db :jdbc-flags)))

(defn- sparksql-databricks-v2
  "Create a database specification for a Spark SQL database."
  [{:keys [host port http-path jdbc-flags app-id app-secret catalog db]
    :or   {host "localhost", port 10000, db "", jdbc-flags ""}
    :as   opts}]
  (merge
   {:classname   "metabase.driver.FixedSparkDriver"
    :subprotocol "databricks"
    :subname     (str "//" host ":" port jdbc-flags)
    :ssl         1
    :httpPath    http-path
    :ConnSchema  db
    :ConnCatalog catalog
    :AuthMech    11
    :Auth_Flow   1
    :OAuth2ClientId app-id
    :OAuth2Secret app-secret}
   (dissoc opts :host :port :db :jdbc-flags :http-path :app-id :app-secret :catalog)))

(defmethod sql-jdbc.conn/connection-details->spec :sparksql-databricks-v2
  [_ details]
  (-> details
      (update :port (fn [port]
                      (if (string? port)
                        (Integer/parseInt port)
                        port)))
      (set/rename-keys {:dbname :db})
      (select-keys [:host :port :db :jdbc-flags :dbname :http-path :app-id :app-secret :catalog])
      sparksql-databricks-v2
      (sql-jdbc.common/handle-additional-options details)))

(defn- dash-to-underscore [s]
  (when s
    (str/replace s #"-" "_")))

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
;; (defmethod driver/describe-database :sparksql-databricks-v2
;;   [_ database]
;;   {:tables
;;    (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
;;      (set
;;       (for [{:keys [database tablename tab_name table_name table_schema], table-namespace :namespace} (jdbc/query {:connection conn} ["select * from information_schema.tables"])]
;;         {:name   (or tablename tab_name table_name table_schema) ; column name differs depending on server (SparkSQL, hive, Impala)
;;          :schema (or (not-empty database)
;;                       (not-empty table_schema)
;;                      (not-empty table-namespace))})))})

(defmethod sql-jdbc.sync/describe-fields-sql :sparksql-databricks-v2
  [driver & {:keys [schema-names table-names catalog]}]
  (assert (string? (not-empty catalog)) "`catalog` is required for sync.")
  (sql/format {:select [[:c.column_name :name]
                        [:c.full_data_type :database-type]
                        [:c.ordinal_position :database-position]
                        [:c.table_schema :table-schema]
                        [:c.table_name :table-name]
                        [[:case [:= :cs.constraint_type [:inline "PRIMARY KEY"]] true :else false] :pk?]
                        [[:case [:not= :c.comment [:inline ""]] :c.comment :else nil] :field-comment]]
               :from [[:system.information_schema.columns :c]]
               ;; Following links contains contains diagram of `information_schema`:
               ;; https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html
               :left-join [[{:select   [[:tc.table_catalog :table_catalog]
                                        [:tc.table_schema :table_schema]
                                        [:tc.table_name :table_name]
                                        [:ccu.column_name :column_name]
                                        [:tc.constraint_type :constraint_type]]
                             :from     [[:system.information_schema.table_constraints :tc]]
                             :join     [[:system.information_schema.constraint_column_usage :ccu]
                                        [:and
                                         [:= :tc.constraint_catalog :ccu.constraint_catalog]
                                         [:= :tc.constraint_schema :ccu.constraint_schema]
                                         [:= :tc.constraint_name :ccu.constraint_name]]]
                             :where [:= :tc.constraint_type [:inline "PRIMARY KEY"]]
                             ;; In case on pk constraint is used by multiple columns this query would return duplicate
                             ;; rows. Group by ensures all rows are distinct. This may not be necessary, but rather
                             ;; safe than sorry.
                             :group-by [:tc.table_catalog
                                        :tc.table_schema
                                        :tc.table_name
                                        :ccu.column_name
                                        :tc.constraint_type]}
                            :cs]
                           [:and
                            [:= :c.table_catalog :cs.table_catalog]
                            [:= :c.table_schema :cs.table_schema]
                            [:= :c.table_name :cs.table_name]
                            [:= :c.column_name :cs.column_name]]]
               :where [:and
                       [:= :c.table_catalog [:inline catalog]]
                       ;; Ignore `timestamp_ntz` type columns. Columns of this type are not recognizable from
                       ;; `timestamp` columns when fetching the data. This exception should be removed when the problem
                       ;; is resolved by Databricks in underlying jdbc driver.
                       [:not= :c.full_data_type [:inline "timestamp_ntz"]]
                       [:not [:in :c.table_schema ["information_schema"]]]
                       (when schema-names [:in :c.table_schema schema-names])
                       (when table-names [:in :c.table_name table-names])]
               :order-by [:table-schema :table-name :database-position]}
              :dialect (sql.qp/quote-style driver)))

(defmethod driver/describe-fields :sql-jdbc
  [driver database & {:as args}]
  (let [catalog (get-in database [:details :catalog])]
    (sql-jdbc.sync/describe-fields driver database (assoc args :catalog catalog))))

(defmethod sql-jdbc.sync/describe-fks-sql :sparksql-databricks-v2
  [driver & {:keys [schema-names table-names catalog]}]
  (assert (string? (not-empty catalog)) "`catalog` is required for sync.")
  (sql/format {:select (vec
                        {:fk_kcu.table_schema  "fk-table-schema"
                         :fk_kcu.table_name    "fk-table-name"
                         :fk_kcu.column_name   "fk-column-name"
                         :pk_kcu.table_schema  "pk-table-schema"
                         :pk_kcu.table_name    "pk-table-name"
                         :pk_kcu.column_name   "pk-column-name"})
               :from [[:system.information_schema.key_column_usage :fk_kcu]]
               :join [[:system.information_schema.referential_constraints :rc]
                      [:and
                       [:= :fk_kcu.constraint_catalog :rc.constraint_catalog]
                       [:= :fk_kcu.constraint_schema :rc.constraint_schema]
                       [:= :fk_kcu.constraint_name :rc.constraint_name]]
                      [:system.information_schema.key_column_usage :pk_kcu]
                      [[:and
                        [:= :pk_kcu.constraint_catalog :rc.unique_constraint_catalog]
                        [:= :pk_kcu.constraint_schema :rc.unique_constraint_schema]
                        [:= :pk_kcu.constraint_name :rc.unique_constraint_name]]]]
               :where [:and
                       [:= :fk_kcu.table_catalog [:inline catalog]]
                       [:not [:in :fk_kcu.table_schema ["information_schema"]]]
                       (when table-names [:in :fk_kcu.table_name table-names])
                       (when schema-names [:in :fk_kcu.table_schema schema-names])]
               :order-by [:fk-table-schema :fk-table-name]}
              :dialect (sql.qp/quote-style driver)))

(defmethod driver/describe-fks :sql-jdbc
  [driver database & {:as args}]
  (let [catalog (get-in database [:details :catalog])]
    (sql-jdbc.sync/describe-fks driver database (assoc args :catalog catalog))))

(defmethod sql-jdbc.execute/set-timezone-sql :sparksql-databricks-v2
  [_driver]
  "SET TIME ZONE %s;")

(defmethod driver/db-default-timezone :sparksql-databricks-v2
  [driver database]
  (sql-jdbc.execute/do-with-connection-with-options
   driver database nil
   (fn [^Connection conn]
     (with-open [stmt (.prepareStatement conn "select current_timezone()")
                 rset (.executeQuery stmt)]
       (when (.next rset)
         (.getString rset 1))))))

;; Hive describe table result has commented rows to distinguish partitions
(defn- valid-describe-table-row? [{:keys [col_name data_type]}]
  (every? (every-pred (complement str/blank?)
                      (complement #(str/starts-with? % "#")))
          [col_name data_type]))

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
;; (defmethod driver/describe-table :sparksql-databricks-v2
;;   [driver database {table-name :name, schema :schema}]
;;   {:name   table-name
;;    :schema schema
;;    :fields
;;    (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
;;      (let [results (jdbc/query {:connection conn} [(format
;;                                                     "describe %s"
;;                                                     (sql.u/quote-name driver :table
;;                                                                       (dash-to-underscore schema)
;;                                                                       (dash-to-underscore table-name)))])]
;;        (set
;;         (for [[idx {col-name :col_name, data-type :data_type, :as result}] (m/indexed results)
;;               :when (valid-describe-table-row? result)]
;;           {:name              col-name
;;            :database-type     data-type
;;            :base-type         (sql-jdbc.sync/database-type->base-type :hive-like (keyword data-type))
;;            :database-position idx}))))})

;; bound variables are not supported in Spark SQL (maybe not Hive either, haven't checked)
;; (defmethod driver/execute-reducible-query :sparksql-databricks-v2
;;   [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
;;   (let [inner-query (-> (assoc inner-query
;;                                :remark (qputil/query->remark :sparksql-databricks-v2 outer-query)
;;                                :query  (if (seq params)
;;                                          (binding [hive-like/*param-splice-style* :paranoid]
;;                                            (unprepare/unprepare driver (cons sql params)))
;;                                          sql)
;;                                :max-rows (mbql.u/query->max-rows-limit outer-query))
;;                         (dissoc :params))
;;         query       (assoc outer-query :native inner-query)]
;;     ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))

;; 1.  SparkSQL doesn't support `.supportsTransactionIsolationLevel`
;; 2.  SparkSQL doesn't support session timezones (at least our driver doesn't support it)
;; 3.  SparkSQL doesn't support making connections read-only
;; 4.  SparkSQL doesn't support setting the default result set holdability
(defmethod sql-jdbc.execute/do-with-connection-with-options :sparksql-databricks-v2
  [driver db-or-id-or-spec options f]
  (sql-jdbc.execute/do-with-resolved-connection
   driver
   db-or-id-or-spec
   options
   (fn [^Connection conn]
     (when-not (sql-jdbc.execute/recursive-connection?)
       (.setTransactionIsolation conn Connection/TRANSACTION_READ_UNCOMMITTED))
     (f conn))))

;; 1.  SparkSQL doesn't support setting holdability type to `CLOSE_CURSORS_AT_COMMIT`
(defmethod sql-jdbc.execute/prepared-statement :sparksql-databricks-v2
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

;; the current HiveConnection doesn't support .createStatement
(defmethod sql-jdbc.execute/statement-supported? :sparksql-databricks-v2 [_] false)

(doseq [[feature supported?] {:basic-aggregations               true
                              :binning                          true
                              :expression-aggregations          true
                              :expressions                      true
                              :describe-fks                     true
                              :native-parameters                true
                              :nested-queries                   true
                              :standard-deviation-aggregations  true
                              :foreign-keys                     true
                              :full-join                        true
                              :right-join                       true
                              :left-join                        true
                              :inner-join                       true
                              :window-functions/offset          false}]
  (defmethod driver/database-supports? [:sparksql-databricks-v2 feature] [_driver _feature _db] supported?))

;; only define an implementation for `:foreign-keys` if none exists already. In test extensions we define an alternate
;; implementation, and we don't want to stomp over that if it was loaded already
(when-not (get (methods driver/database-supports?) [:sparksql-databricks-v2 :foreign-keys])
  (defmethod driver/database-supports? [:sparksql-databricks-v2 :foreign-keys] [_driver _feature _db] false))

(defmethod sql.qp/quote-style :sparksql-databricks-v2 [_] :mysql)

(def ^:private timestamp-database-type-names #{"TIMESTAMP" "TIMESTAMP_NTZ"})

;; Both timestamp types, TIMESTAMP and TIMESTAMP_NTZ, are returned in `Types/TIMESTAMP` sql type. TIMESTAMP is wall
;; clock with date in session timezone. Hence the following implementation adds the results timezone in LocalDateTime
;; gathered from JDBC driver and then adjusts the value to ZULU. Presentation tweaks (ie. changing to report for users'
;; pleasure) are done in `wrap-value-literals` middleware.
(defmethod sql-jdbc.execute/read-column-thunk [:sparksql-databricks-v2 java.sql.Types/TIMESTAMP]
  [_driver ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  ;; TIMESTAMP is returned also for TIMESTAMP_NTZ type!!! Hence only true branch is hit until this is fixed upstream.
  (let [database-type-name (.getColumnTypeName rsmeta i)]
    (assert (timestamp-database-type-names database-type-name))
    (if (= "TIMESTAMP" database-type-name)
      (fn []
        (assert (some? (qp.timezone/results-timezone-id)))
        (when-let [t (.getTimestamp rs i)]
          (t/with-offset-same-instant
            (t/offset-date-time
             (t/zoned-date-time (t/local-date-time t)
                                (t/zone-id (qp.timezone/results-timezone-id))))
            (t/zone-id "Z"))))
      (fn []
        (when-let [t (.getTimestamp rs i)]
          (t/local-date-time t))))))

(defn- date-time->results-local-date-time
  "For datetime types with zone info generate LocalDateTime as in that zone. Databricks java driver does not support
  setting OffsetDateTime or ZonedDateTime parameters. It uses parameters as in session timezone. Hence, this function
  shifts LocalDateTime so wall clock corresponds to Databricks' timezone."
  [dt]
  (if (instance? LocalDateTime dt)
    dt
    (let [tz-str      (try (qp.timezone/results-timezone-id)
                           (catch Throwable _
                             (log/trace "Failed to get `results-timezone-id`. Using system timezone.")
                             (qp.timezone/system-timezone-id)))
          adjusted-dt (t/with-zone-same-instant (t/zoned-date-time dt) (t/zone-id tz-str))]
      (t/local-date-time adjusted-dt))))

(defn- set-parameter-to-local-date-time
  [driver prepared-statement index object]
  ((get-method sql-jdbc.execute/set-parameter [::sql-jdbc.legacy/use-legacy-classes-for-read-and-set LocalDateTime])
   driver prepared-statement index (date-time->results-local-date-time object)))

(defmethod sql-jdbc.execute/set-parameter [:sparksql-databricks-v2 OffsetDateTime]
  [driver prepared-statement index object]
  (set-parameter-to-local-date-time driver prepared-statement index object))

(defmethod sql-jdbc.execute/set-parameter [:sparksql-databricks-v2 ZonedDateTime]
  [driver prepared-statement index object]
  (set-parameter-to-local-date-time driver prepared-statement index object))

;;
;; `set-parameter` is implmented also for LocalTime and OffsetTime, even though Databricks does not support time types.
;; It enables creation of `attempted-murders` dataset, hence making the driver compatible with more of existing tests.
;;

(defmethod sql-jdbc.execute/set-parameter [:sparksql-databricks-v2 LocalTime]
  [driver prepared-statement index object]
  (set-parameter-to-local-date-time driver prepared-statement index
                                    (t/local-date-time (t/local-date 1970 1 1) object)))

(defmethod sql-jdbc.execute/set-parameter [:sparksql-databricks-v2 OffsetTime]
  [driver prepared-statement index object]
  (set-parameter-to-local-date-time driver prepared-statement index
                                    (t/local-date-time (t/local-date 1970 1 1) object)))