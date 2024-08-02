(ns metabase.driver.maxcompute
  (:require
    [cheshire.core :as json]
    [clojure.core]
    [clojure.string :as str]
    [honey.sql :as hsql]
    [java-time.api :as t]
    [metabase.driver :as driver]
    [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
    [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
    [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
    [metabase.driver.sql.query-processor :as sql.qp]
    [metabase.util.date-2 :as u.date]
    [metabase.util.honey-sql-2 :as h2x])
  (:import
    (java.sql Connection ResultSet Time)
    (java.time LocalDate LocalDateTime OffsetDateTime ZonedDateTime)
    (java.util Date)
    (com.aliyun.odps Column Table Project Odps OdpsException)
    (com.aliyun.odps.jdbc OdpsConnection)
    (com.aliyun.odps.account AliyunAccount)))

(set! *warn-on-reflection* true)

(driver/register! :maxcompute, :parent :sql-jdbc)
(doseq [[feature supported?] {;; Does this database support following foreign key relationships while querying?
                              ;; Note that this is different from supporting primary key and foreign key constraints in the schema; see below.
                              :foreign-keys                           false

                              ;; Does this database track and enforce primary key and foreign key constraints in the schema?
                              ;; SQL query engines like Presto and Athena do not track these, though they can query across FKs.
                              ;; See :foreign-keys above.
                              :metadata/key-constraints               false

                              ;; Does this database support nested fields for any and every field except primary key (e.g. Mongo)?
                              :nested-fields                          false

                              ;; Does this database support nested fields but only for certain field types (e.g. Postgres and JSON / JSONB columns)?
                              :nested-field-columns                   true

                              ;; Does this driver support setting a timezone for the query?
                              :set-timezone                           false

                              ;; Does the driver support *basic* aggregations like `:count` and `:sum`? (Currently, everything besides standard
                              ;; deviation is considered \"basic\"; only GA doesn't support this).
                              :basic-aggregations                     true

                              ;; Does this driver support standard deviation and variance aggregations? Note that if variance is not supported
                              ;; directly, you can calculate it manually by taking the square of the standard deviation. See the MongoDB driver
                              ;; for example.
                              :standard-deviation-aggregations        false

                              ;; Does this driver support expressions (e.g. adding the values of 2 columns together)?
                              :expressions                            true

                              ;; Does this driver support parameter substitution in native queries, where parameter expressions are replaced
                              ;; with a single value? e.g.
                              ;;
                              ;;    SELECT * FROM table WHERE field = {{param}}
                              ;;    ->
                              ;;    SELECT * FROM table WHERE field = 1
                              :native-parameters                      true

                              ;; Does the driver support using expressions inside aggregations? e.g. something like \"sum(x) + count(y)\" or
                              ;; \"avg(x + y)\"
                              :expression-aggregations                true

                              ;; Does the driver support using a query as the `:source-query` of another MBQL query? Examples are CTEs or
                              ;; subselects in SQL queries.
                              :nested-queries                         false

                              ;; Does this driver support native template tag parameters of type `:card`, e.g. in a native query like
                              ;;
                              ;;    SELECT * FROM {{card}}
                              ;;
                              ;; do we support substituting `{{card}}` with another compiled (nested) query?
                              ;;
                              ;; By default, this is true for drivers that support `:native-parameters` and `:nested-queries`, but drivers can opt
                              ;; out if they do not support Card ID template tag parameters.
                              :native-parameter-card-reference        false

                              ;; Does the driver support persisting models
                              :persist-models                         false
                              ;; Is persisting enabled?
                              :persist-models-enabled                 false

                              ;; Does the driver support binning as specified by the `binning-strategy` clause?
                              :binning                                false

                              ;; Does this driver not let you specify whether or not our string search filter clauses (`:contains`,
                              ;; `:starts-with`, and `:ends-with`, collectively the equivalent of SQL `LIKE`) are case-senstive or not? This
                              ;; informs whether we should present you with the 'Case Sensitive' checkbox in the UI. At the time of this writing
                              ;; SQLite, SQLServer, and MySQL do not support this -- `LIKE` clauses are always case-insensitive.
                              ;;
                              ;; DEFAULTS TO TRUE.
                              :case-sensitivity-string-filter-options false

                              :left-join                              true
                              :right-join                             true
                              :inner-join                             true
                              :full-join                              false

                              :regex                                  false

                              ;; Does the driver support advanced math expressions such as log, power, ...
                              :advanced-math-expressions              false

                              ;; Does the driver support percentile calculations (including median)
                              :percentile-aggregations                false

                              ;; Does the driver support date extraction functions? (i.e year('1970/03/09'))
                              ;; DEFAULTS TO TRUE
                              :temporal-extract                       true

                              ;; Does the driver support doing math with datetime? (i.e Adding 1 year to a datetime column)
                              ;; DEFAULTS TO TRUE e.g. dateadd(datetime '2005-03-30 00:00:00', -1, 'mm');
                              :date-arithmetics                       true

                              ;; Does the driver support the :now function
                              :now                                    true

                              ;; Does the driver support converting timezone?
                              ;; DEFAULTS TO FALSE
                              :convert-timezone                       false

                              ;; Does the driver support :datetime-diff functions
                              :datetime-diff                          true

                              ;; Does the driver support experimental "writeback" actions like "delete this row" or "insert a new row" from 44+?
                              :actions                                false

                              ;; Does the driver support storing table privileges in the application database for the current user?
                              :table-privileges                       false

                              ;; Does the driver support uploading files
                              :uploads                                true

                              ;; Does the driver support schemas (aka namespaces) for tables
                              ;; DEFAULTS TO TRUE
                              :schemas                                false

                              ;; Does the driver support custom writeback actions. Drivers that support this must
                              ;; implement [[execute-write-query!]]
                              :actions/custom                         false

                              ;; Does changing the JVM timezone allow producing correct results? (See #27876 for details.)
                              :test/jvm-timezone-setting              false

                              ;; Does the driver support connection impersonation (i.e. overriding the role used for individual queries)?
                              :connection-impersonation               false

                              ;; Does the driver require specifying the default connection role for connection impersonation to work?
                              :connection-impersonation-requires-role false

                              ;; Does the driver require specifying a collection (table) for native queries? (mongo)
                              :native-requires-specified-collection   false

                              ;; Does the driver support column(s) support storing index info
                              :index-info                             false

                              ;; Does the driver support a faster `sync-fks` step by fetching all FK metadata in a single collection?
                              ;; if so, `metabase.driver/describe-fks` must be implemented instead of `metabase.driver/describe-table-fks`
                              :describe-fks                           false

                              ;; Does the driver support a faster `sync-fields` step by fetching all FK metadata in a single collection?
                              ;; if so, `metabase.driver/describe-fields` must be implemented instead of `metabase.driver/describe-table`
                              :describe-fields                        false

                              ;; Does the driver support automatically adding a primary key column to a table for uploads?
                              ;; If so, Metabase will add an auto-incrementing primary key column called `_mb_row_id` for any table created or
                              ;; updated with CSV uploads, and ignore any `_mb_row_id` column in the CSV file.
                              ;; DEFAULTS TO TRUE
                              :upload-with-auto-pk                    false

                              ;; Does the driver support fingerprint the fields. Default is true
                              :fingerprint                            false

                              ;; Does a connection to this driver correspond to a single database (false), or to multiple databases (true)?
                              ;; Default is false; ie. a single database. This is common for classic relational DBs and some cloud databases.
                              ;; Some have access to many databases from one connection; eg. Athena connects to an S3 bucket which might have
                              ;; many databases in it.
                              :connection/multiple-databases          false

                              ;; Does this driver support window functions like cumulative count and cumulative sum? (default: false)
                              :window-functions/cumulative            true

                              ;; Does this driver support the new `:offset` MBQL clause added in 50? (i.e. SQL `lag` and `lead` or equivalent
                              ;; functions)
                              :window-functions/offset                true
                              }]
  (defmethod driver/database-supports? [:maxcompute feature] [_driver _feature _db] supported?))

(def odps-instance (atom nil))
(defmethod driver/can-connect? :maxcompute
  [driver details]
  (let [{:keys [project endpoint ak sk]} details
        account (AliyunAccount. ak sk)
        odps (Odps. account)]
    (.setEndpoint odps endpoint)
    (.setDefaultProject odps project)
    (try
      (let [projects (.projects odps)]
        (.exists projects project)
        (reset! odps-instance odps)
        true)
      (catch OdpsException e
        (println "driver/can-connect? - exception:" e)
        false))))

;; this convert "a"."b"."c" to `a`.`b`.`c`, which is necessary for maxcompute
(defmethod sql.qp/quote-style :maxcompute [_] :mysql)

(defmethod sql-jdbc.conn/connection-details->spec :maxcompute
  [driver details-map]
  (let [{:keys [project endpoint ak sk timezone settings]} details-map
        ;; 将 MaxCompute SQL 的默认 settings 放在这里
        default-settings {"odps.sql.validate.orderby.limit" "false"
                          "odps.sql.type.system.odps2"      "true"
                          "odps.sql.timezone"               timezone}
        settings-map (merge default-settings (try
                                               (when settings
                                                 (json/parse-string settings true))
                                               (catch Exception e
                                                 (println "Invalid settings JSON" settings)
                                                 {})))]
    (if (or (nil? endpoint) (nil? project) (nil? ak) (nil? sk))
      (throw (IllegalArgumentException. "Missing required connection details"))
      {:classname   "com.aliyun.odps.jdbc.OdpsDriver"
       :subprotocol "odps"
       :subname     (str endpoint "?project=" project
                         "&enableOdpsLogger=true&charset=UTF-8&interactiveMode=true&enableLimit=false"
                         "&settings=" (json/generate-string settings-map))
       :user        ak
       :password    sk})))
(defmethod driver/describe-database :maxcompute
  [driver database]
  (let [odps @odps-instance
        tables-it (.iterator (.tables odps))]
    (loop [tables-metadata #{}]
      (if (.hasNext tables-it)
        (let [table (.next tables-it)
              table-metadata {:name                    (.getName table)
                              :schema                  (.getDefaultProject odps)
                              :description             (.getComment table)
                              :database_require_filter (.isPartitioned table)}]
          (println "table-metadata:" table-metadata)
          (recur (conj tables-metadata table-metadata)))
        {:tables tables-metadata}))))
(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
    [[#"BIGINT" :type/BigInteger]
     [#"TINYINT" :type/Integer]
     [#"SMALLINT" :type/Integer]
     [#"INT" :type/Integer]
     [#"CHAR" :type/Text]
     [#"STRING" :type/Text]
     [#"JSON" :type/Text]
     [#"VARCHAR" :type/Text]
     [#"BINARY" :type/*]
     [#"FLOAT" :type/Float]
     [#"DOUBLE" :type/Float]
     [#"DECIMAL" :type/Decimal]
     [#"BOOLEAN" :type/Boolean]
     [#"TIMESTAMP" :type/DateTime]
     [#"TIMESTAMP_NTZ" :type/DateTime]
     [#"DATETIME" :type/DateTime]
     [#"DATE" :type/Date]
     [#"ARRAY" :type/*]
     [#"MAP" :type/*]
     [#"STRUCT" :type/*]

     ]))
(defmethod sql-jdbc.sync/database-type->base-type :maxcompute
  [_ database-type]
  (database-type->base-type database-type))

;; maxcompute's JDBC driver is fussy and won't let you change connections to read-only after you create them. So skip that
;; step. maxcompute doesn't have a notion of session timezones so don't do that either. The only thing we're doing here from
;; the default impl is setting the transaction isolation level
(defmethod sql-jdbc.execute/do-with-connection-with-options :maxcompute
  [driver db-or-id-or-spec options f]
  (sql-jdbc.execute/do-with-resolved-connection
    driver
    db-or-id-or-spec
    options
    (fn [^Connection conn]
      (f conn))))

;; maxcompute's JDBC driver is dumb and complains if you try to call `.setFetchDirection` on the Connection
(defmethod sql-jdbc.execute/prepared-statement :maxcompute
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))
(defmethod sql.qp/->honeysql [:maxcompute :datetime-diff]
  [driver [_ x y unit]]
  (let [x (sql.qp/->honeysql driver x)
        y (sql.qp/->honeysql driver y)]
    (:raw (if (nil? unit)
      (str "DATEDIFF(" x ", " y ")")
      (str "DATEDIFF(" x ", " y ", '-" unit "')")))))

;; below code is copy from https://github.com/jess0018/metabase-odps-driver/blob/master/src/metabase/driver/odps.clj
(defmethod sql.qp/current-datetime-honeysql-form :maxcompute [_] (:raw "getdate()"))
(defmethod sql.qp/unix-timestamp->honeysql [:maxcompute :seconds]
  [_ _ expr]
  (h2x/->timestamp (hsql/call :from_unixtime expr)))

(defn- date-format [format-str expr]
  (hsql/call :to_char expr (h2x/literal format-str)))

(defn- str-to-date [format-str expr] (hsql/call :to_date expr (h2x/literal format-str)))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str (h2x/cast :DATETIME expr))))

(defmethod sql.qp/date [:maxcompute :second] [_ _ expr] (trunc-with-format "yyyy-mm-dd hh:mi:ss" expr))
(defmethod sql.qp/date [:maxcompute :minute] [_ _ expr] (trunc-with-format "yyyy-mm-dd hh:mi" expr))
(defmethod sql.qp/date [:maxcompute :minute-of-hour] [_ _ expr] (hsql/call :datepart expr (h2x/literal "mi")))
(defmethod sql.qp/date [:maxcompute :hour] [_ _ expr] (trunc-with-format "yyyy-mm-dd hh" expr))
(defmethod sql.qp/date [:maxcompute :hour-of-day] [_ _ expr] (hsql/call :datepart expr (h2x/literal "hh")))
(defmethod sql.qp/date [:maxcompute :day] [_ _ expr] (trunc-with-format "yyyy-mm-dd" expr))
(defmethod sql.qp/date [:maxcompute :day-of-week] [_ _ expr] (h2x/+ (hsql/call :weekday expr) 1))
(defmethod sql.qp/date [:maxcompute :day-of-month] [_ _ expr] (hsql/call :datepart expr (h2x/literal "dd")))
(defmethod sql.qp/date [:maxcompute :week-of-year] [_ _ expr] (hsql/call :weekofyear expr))
(defmethod sql.qp/date [:maxcompute :month] [_ _ expr] (trunc-with-format "yyyy-mm" expr))
(defmethod sql.qp/date [:maxcompute :month-of-year] [_ _ expr] (hsql/call :datepart expr (h2x/literal "mm")))
(defmethod sql.qp/date [:maxcompute :year] [_ _ expr] (trunc-with-format "yyyy" expr))

(defmethod sql.qp/date [:maxcompute :quarter] [_ _ expr]
  (hsql/call :dateadd (hsql/call :datetrunc expr (h2x/literal "yyyy"))
             (h2x/* (h2x/- ((hsql/call :quarter expr)) 1) 3)
             (h2x/literal "mm")
             ))

(defmethod sql.qp/date [:maxcompute :quarter-of-year] [_ _ expr]
  (hsql/call :ceil (h2x// (hsql/call :datepart expr (h2x/literal "mm")) 3)))


(prefer-method sql.qp/inline-value [:sql Time] [:maxcompute Date])

(defmethod sql.qp/inline-value [:maxcompute String] [_ value]
  (str \' (str/replace value "'" "\\\\'") \'))

(defmethod sql.qp/inline-value [:odps LocalDate]
  [driver t]
  (sql.qp/inline-value driver (t/local-date-time t (t/local-time 0))))

(defmethod sql.qp/inline-value [:odps LocalDateTime]
  [_ t]
  (format "to_date('%s', 'yyyy-mm-dd hh:mi:ss')" (u.date/format-sql (t/local-date-time t))))

(defmethod sql.qp/inline-value [:odps OffsetDateTime]
  [_ t]
  (format "to_date('%s', 'yyyy-mm-dd hh:mi:ss')" (u.date/format-sql (t/local-date-time t))))

(defmethod sql.qp/inline-value [:odps ZonedDateTime]
  [_ t]
  (format "to_date('%s', 'yyyy-mm-dd hh:mi:ss')" (u.date/format-sql (t/local-date-time t))))
