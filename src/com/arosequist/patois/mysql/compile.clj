(ns com.arosequist.patois.mysql.compile
  (:require [com.arosequist.patois.mysql.schema :as schema]
            [schema.core :refer [validate]]
            [clojure.string :as str]))

(def reserved-words #{"ADD" "ALL" "ALTER" "ANALYZE" "AND" "AS" "ASC" "ASENSITIVE" "BEFORE" "BETWEEN" "BIGINT" "BINARY" "BLOB" "BOTH" "BY" "CALL" "CASCADE" "CASE" "CHANGE" "CHAR" "CHARACTER" "CHECK" "COLLATE" "COLUMN" "CONDITION" "CONSTRAINT" "CONTINUE" "CONVERT" "CREATE" "CROSS" "CURRENT_DATE" "CURRENT_TIME" "CURRENT_TIMESTAMP" "CURRENT_USER" "CURSOR" "DATABASE" "DATABASES" "DAY_HOUR" "DAY_MICROSECOND" "DAY_MINUTE" "DAY_SECOND" "DEC" "DECIMAL" "DECLARE" "DEFAULT" "DELAYED" "DELETE" "DESC" "DESCRIBE" "DETERMINISTIC" "DISTINCT" "DISTINCTROW" "DIV" "DOUBLE" "DROP" "DUAL" "EACH" "ELSE" "ELSEIF" "ENCLOSED" "ESCAPED" "EXISTS" "EXIT" "EXPLAIN" "FALSE" "FETCH" "FLOAT" "FLOAT4" "FLOAT8" "FOR" "FORCE" "FOREIGN" "FROM" "FULLTEXT" "GRANT" "GROUP" "HAVING" "HIGH_PRIORITY" "HOUR_MICROSECOND" "HOUR_MINUTE" "HOUR_SECOND" "IF" "IGNORE" "IN" "INDEX" "INFILE" "INNER" "INOUT" "INSENSITIVE" "INSERT" "INT" "INT1" "INT2" "INT3" "INT4" "INT8" "INTEGER" "INTERVAL" "INTO" "IS" "ITERATE" "JOIN" "KEY" "KEYS" "KILL" "LEADING" "LEAVE" "LEFT" "LIKE" "LIMIT" "LINES" "LOAD" "LOCALTIME" "LOCALTIMESTAMP" "LOCK" "LONG" "LONGBLOB" "LONGTEXT" "LOOP" "LOW_PRIORITY" "MATCH" "MEDIUMBLOB" "MEDIUMINT" "MEDIUMTEXT" "MIDDLEINT" "MINUTE_MICROSECOND" "MINUTE_SECOND" "MOD" "MODIFIES" "NATURAL" "NOT" "NO_WRITE_TO_BINLOG" "NULL" "NUMERIC" "ON" "OPTIMIZE" "OPTION" "OPTIONALLY" "OR" "ORDER" "OUT" "OUTER" "OUTFILE" "PRECISION" "PRIMARY" "PROCEDURE" "PURGE" "READ" "READS" "REAL" "REFERENCES" "REGEXP" "RELEASE" "RENAME" "REPEAT" "REPLACE" "REQUIRE" "RESTRICT" "RETURN" "REVOKE" "RIGHT" "RLIKE" "SCHEMA" "SCHEMAS" "SECOND_MICROSECOND" "SELECT" "SENSITIVE" "SEPARATOR" "SET" "SHOW" "SMALLINT" "SONAME" "SPATIAL" "SPECIFIC" "SQL" "SQLEXCEPTION" "SQLSTATE" "SQLWARNING" "SQL_BIG_RESULT" "SQL_CALC_FOUND_ROWS" "SQL_SMALL_RESULT" "SSL" "STARTING" "STRAIGHT_JOIN" "TABLE" "TERMINATED" "THEN" "TINYBLOB" "TINYINT" "TINYTEXT" "TO" "TRAILING" "TRIGGER" "TRUE" "UNDO" "UNION" "UNIQUE" "UNLOCK" "UNSIGNED" "UPDATE" "USAGE" "USE" "USING" "UTC_DATE" "UTC_TIME" "UTC_TIMESTAMP" "VALUES" "VARBINARY" "VARCHAR" "VARCHARACTER" "VARYING" "WHEN" "WHERE" "WHILE" "WITH" "WRITE" "XOR" "YEAR_MONTH" "ZEROFILL"})

(defn reserved-word?
  [s]
  (if s
    (contains? reserved-words (.toUpperCase (.trim s)))))

(declare compile-select-statement)

(defn compile-expression
  ([expr]
    (compile-expression expr nil))
  ([expr {:keys [validate? quote-identifiers] :or {validate? true, quote-identifiers :when-needed} :as opts}]
    (if validate?
      (validate schema/Expression expr))
    (case (:type expr)
      :string
        (str
          (if (:charset expr) (str "_" (:charset expr)))
          "'" (:value expr) "'")
      :number
        (str (:value expr))
      :date
        (let [{:keys [year month date hour minute second]} expr
              pad (partial format "%02d")]
          (str
            "'" year "-" (pad month) "-" (pad date)
            (if hour (str " " (pad hour)))
            (if minute (str ":" (pad minute)))
            (if second
              (if (integer? second)
                (str ":" (pad second))
                (str ":" (format "%09.6f" (float second)))))
            "'"))
      :time
        (let [{:keys [days hours minutes seconds]} expr
              pad (partial format "%02d")]
          (str
            "'"
            (if days (str days " "))
            (if hours (pad hours))
            (if (and hours minutes) ":")
            (if minutes (pad minutes))
            (if (and minutes seconds) ":")
            (if seconds
              (if (integer? seconds)
                (pad seconds)
                (format "%09.6f" (float seconds))))
            "'"))
      :boolean
        (if (:value expr) "TRUE" "FALSE")
      :bitfield
        (str "b'" (:value expr) "'")
      :null
        "NULL"
      :column
        (let [{:keys [schema table column]} expr
              quote-id #(if (or (= quote-identifiers :always)
                                (reserved-word? %))
                          (str "`" % "`")
                          %)]
          (str
            (if schema (str (quote-id schema) "."))
            (if table (str (quote-id table) "."))
            (quote-id column)))
      :function-call
        (let [{:keys [schema name params]} expr]
          (str
            (if schema (str schema "."))
            name
            "(" (str/join ", " (map #(compile-expression % opts) params)) ")"))
      :group-function-call
        (let [{:keys [name star? distinct? expression]} expr]
          (str
            name "("
            (if star? "*")
            (if distinct? "DISTINCT ")
            (if expression (compile-expression expression opts))
            ")"))
      :cast
        (let [{:keys [expression to]} expr
              {:keys [type length precision scale]} to]
          (str
            "CAST("
            (compile-expression expression opts)
            " AS "
            type
            (if (or length precision scale) "(")
            (if length length)
            (if precision precision)
            (if scale (str ", " scale))
            (if (or length precision scale) ")")
            ")"))
      :convert
        (let [{:keys [expression transcoding-name]} expr]
          (str
            "CONVERT("
            (compile-expression expression opts)
            " USING "
            transcoding-name
            ")"))
      :collation
        (let [{:keys [expression collation-name]} expr]
          (str
            (compile-expression expression opts)
            " COLLATE "
            collation-name))
      :binary
        (str "BINARY " (compile-expression (:expression expr) opts))
      :concatenation
        (str/join " || " (map #(compile-expression % opts) (:expressions expr)))
      :parameter
        "?"
      :variable
        (str "@" (:name expr))
      :negation
        (str "-" (compile-expression (:expression expr) opts))
      :exists
        (str
          (if (:not? expr) "NOT ")
          "EXISTS ("
          (compile-select-statement (:subquery expr) opts)
          ")")
      :subquery
        (str
          "("
          (compile-select-statement (:subquery expr) opts)
          ")")
      :match
        (let [{:keys [columns against search-modifier]} expr]
          (str
            "MATCH ("
            (str/join ", " (map #(compile-expression % opts) columns))
            ") AGAINST ("
            (compile-expression against opts)
            ")"
            (case search-modifier
              :in-natural-language-mode " IN NATURAL LANGUAGE MODE"
              :in-natural-language-mode-with-query-expansion " IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION"
              :in-boolean-mode " IN BOOLEAN MODE"
              :with-query-expansion " WITH QUERY EXPANSION"
              "")))
      :case
        (let [{:keys [value conditionals]} expr]
          (str
            "CASE "
            (if value (str (compile-expression value opts) " "))
            (str/join " " (map (fn [{:keys [when then]}]
                                 (str
                                   (if (= when :else)
                                     "ELSE "
                                     (str
                                       "WHEN "
                                       (compile-expression when opts)
                                       " THEN "))
                                   (compile-expression then opts)))
                            conditionals))
            " END CASE"))
      :comparison
        (str/join
          (str
            " "
            (case (:operator expr)
              :equals "="
              :not-equals "<>"
              :less-than "<"
              :less-than-or-equal-to "<="
              :greater-than ">"
              :greater-than-or-equal-to ">=")
            " ")
          (map #(compile-expression % opts) (:expressions expr)))
      :interval
        (str
          "INTERVAL "
          (compile-expression (:expression expr) opts)
          " "
          (case (:unit expr)
            :microseconds "MICROSECOND"
            :seconds "SECOND"
            :minutes "MINUTE"
            :hours "HOUR"
            :days "DAY"
            :weeks "WEEK"
            :months "MONTH"
            :quarters "QUARTER"
            :years "YEAR"
            :second-microseconds "SECOND_MICROSECOND"
            :minute-microseconds "MINUTE_MICROSECOND"
            :minute-seconds "MINUTE_SECOND"
            :hour-microseconds "HOUR_MICROSECOND"
            :hour-seconds "HOUR_SECOND"
            :hour-minutes "HOUR_MINUTE"
            :day-microseconds "DAY_MICROSECOND"
            :day-seconds "DAY_SECOND"
            :day-minutes "DAY_MINUTE"
            :day-hours "DAY_HOUR"
            :year-months "YEAR_MONTH"))
      :operation
        (str/join
          (str
            " "
            (case (:operation expr)
              :bit-or "|"
              :bit-and "&"
              :shift-left "<<"
              :shift-right ">>"
              :addition "+"
              :subtraction "-"
              :multiplication "*"
              :division "/"
              :integer-division "DIV"
              :modulus "%"
              :exponentiation "^")
            " ")
          (map
            (fn [e]
              (str
                (if (= (:type e) :operation) "(")
                (compile-expression e opts)
                (if (= (:type e) :operation) ")")))
            (:operands expr)))
      :in
        (str (compile-expression (:expression expr) opts)
          (if (:not? expr) " NOT")
          " IN ("
          (str/join ", " (map #(compile-expression % opts) (:in expr)))
          ")")
      :between
        (str (compile-expression (:expression expr) opts)
          (if (:not? expr) " NOT")
          " BETWEEN "
          (compile-expression (first (:between expr)) opts)
          " AND "
          (compile-expression (second (:between expr)) opts))
      :like
        (str
          (compile-expression (:expression expr) opts)
          (if (:not? expr) " NOT")
          " LIKE "
          (compile-expression (:like expr) opts)
          (if (:escape expr) (str " ESCAPE " (compile-expression (:escape expr) opts))))
      :sounds-like
        (str
          (compile-expression (:expression expr) opts)
          " SOUNDS LIKE "
          (compile-expression (:sounds-like expr) opts))
      :regexp
        (str
          (compile-expression (:expression expr) opts)
          (if (:not? expr) " NOT")
          " REGEXP "
          (compile-expression (:regexp expr) opts))
      :is-null
        (str
          (compile-expression (:expression expr) opts)
          " IS"
          (if (:not? expr) " NOT")
          " NULL")
      :logical
        (str/join
          (case (:operation expr)
            :and " AND "
            :or " OR "
            :xor " XOR "
            (throw (RuntimeException. (str "Unknown logical operation " (:operation expr)))))
          (map #(compile-expression % opts) (:expressions expr)))
      :not
        (str "NOT " (compile-expression (:expression expr) opts))
      (throw (RuntimeException. (str "Unknown expression type: " (:type expr)))))))

(defn compile-order-by-clause
  [clause opts]
  (str " ORDER BY " (str/join ", " (map (fn [{:keys [expression asc-desc]}]
                                          (str
                                            (compile-expression expression opts)
                                            (if (= asc-desc :asc) " ASC")
                                            (if (= asc-desc :desc) " DESC"))) clause))))

(defn compile-select-clause
  [clause opts]
  (let [{:keys [projections options from where group-by group-with-rollup? having order-by limit offset]} clause]
    (str
      "SELECT"
      (if (contains? options :distinct) " DISTINCT")
      (if (contains? options :high-priority) " HIGH_PRIORITY")
      (if (contains? options :straight-join) " STRAIGHT_JOIN")
      (if (contains? options :sql-small-result) " SQL_SMALL_RESULT")
      (if (contains? options :sql-big-result) " SQL_BIG_RESULT")
      (if (contains? options :sql-buffer-result) " SQL_BUFFER_RESULT")
      (if (contains? options :sql-cache) " SQL_CACHE")
      (if (contains? options :sql-no-cache) " SQL_NO_CACHE")
      (if (contains? options :sql-calc-found-rows) " SQL_CALC_FOUND_ROWS")
      (if (= projections :*)
        " *"
        (str
          " "
          (str/join ", " (map
                           (fn [{:keys [expression star alias]}]
                             (str
                               (if expression (compile-expression expression opts))
                               (if star
                                 (str
                                   (if (:schema star) (str (:schema star) "."))
                                   (:table star)
                                   ".*"))
                               (if alias (str " AS " alias))))
                           projections))))
      (if from
        (str
          " FROM"
          (apply str
                 (map-indexed
                   (fn [idx {:keys [schema table subquery natural-join? join-type on using partitions alias index-hints]}]
                     (str
                       (if (and (not (zero? idx)) (= join-type :inner) (nil? on)) ",")
                       (if natural-join? " NATURAL")
                       (if (and join-type on) (str " " ({:inner "JOIN", :straight "STRAIGHT JOIN", :left-outer "LEFT OUTER", :right-outer "RIGHT OUTER"} join-type)))
                       (if schema (str " " schema "." table) (str " " table))
                       (if alias (str " AS " alias))
                       (if index-hints
                         (str
                           " "
                           (str/join ", " (map
                                            (fn [{:keys [action index-key indexes for]}]
                                              (str
                                                (case action
                                                  :use "USE"
                                                  :ignore "IGNORE"
                                                  :force "FORCE")
                                                (if (= index-key :index) " INDEX")
                                                (if (= index-key :key) " KEY")
                                                (if (= for :join) " FOR JOIN")
                                                (if (= for :order-by) " FOR ORDER BY")
                                                (if (= for :group-by) " FOR GROUP BY")
                                                " ("
                                                (str/join ", " indexes)
                                                ")"))
                                            index-hints))))
                       (if on (str " ON " (compile-expression on opts)))
                       (if using (str " USING (" (str/join ", " using) ")"))))
                   from))))
      (if where (str " WHERE " (compile-expression where opts)))
      (if group-by (str " GROUP BY " (str/join ", " (map #(compile-expression % opts) group-by))))
      (if group-with-rollup? (str " WITH ROLLUP"))
      (if having (str " HAVING " (compile-expression having opts)))
      (if order-by (compile-order-by-clause order-by opts))
      (if limit (str " LIMIT " limit))
      (if offset (str " OFFSET " offset)))))

(defn- contains-any?
  [coll keys]
  (some #(contains? coll %) keys))

(defn- selects-need-wrapped?
  [smt]
  (and
    (> (count (:selects smt)) 1)
    (some #(contains-any? (:select %) [:order-by :limit :offset]) (:selects smt))))

(defn compile-select-statement
  ([smt] (compile-select-statement smt nil))
  ([smt {:keys [validate? wrap-selects-in-parens] :or {validate? true, wrap-selects-in-parens :when-needed} :as opts}]
    (if validate?
      (validate schema/SelectStatement smt))
    (let [wrap-selects-in-parens? (case wrap-selects-in-parens
                                    :when-needed (selects-need-wrapped? smt)
                                    :always true)
          {:keys [selects options order-by limit offset]} smt]
      (str
        (str/join " " (map-indexed (fn [idx itm]
                                     (str
                                       (if-not (zero? idx)
                                         (if (:union-all? itm)
                                           "UNION ALL "
                                           "UNION "))
                                       (if wrap-selects-in-parens? "(")
                                       (compile-select-clause (:select itm) opts)
                                       (if wrap-selects-in-parens? ")")))
                                   selects))
        (if order-by (compile-order-by-clause order-by opts))
        (if limit (str " LIMIT " smt))
        (if offset (str " OFFSET " smt))
        (if (contains? options :for-update) " FOR UPDATE")
        (if (contains? options :lock-in-shared-mode) " LOCK IN SHARED MODE")))))