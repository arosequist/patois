(ns com.arosequist.patois.mysql.compile-test
  (:require [com.arosequist.patois.mysql.compile :refer [compile-expression compile-statement]]
            [clojure.test :refer [are deftest is]]))

(deftest literals
  (are [r e] (= r (compile-expression e))
    "'s'" {:type :string, :value "s"}
    "_latin1's'" {:type :string, :value "s", :charset "latin1"}
    "1" {:type :number, :value 1}
    "1.2345" {:type :number, :value 1.2345}
    "'2013-01-02'" {:type :date, :year 2013, :month 1, :date 2}
    "'2013-01-02 03:04:05'" {:type :date, :year 2013, :month 1, :date 2, :hour 3, :minute 4, :second 5}
    "'2013-01-02 03:04:05.678000'" {:type :date, :year 2013, :month 1, :date 2, :hour 3, :minute 4, :second 5.678}
    "'1 02:03:04'" {:type :time, :days 1, :hours 2, :minutes 3, :seconds 4}
    "'1 02:03:04.567000'" {:type :time, :days 1, :hours 2, :minutes 3, :seconds 4.567}
    "'02:03:04'" {:type :time, :hours 2, :minutes 3, :seconds 4}
    "'02:03'" {:type :time, :hours 2, :minutes 3}
    "'1 02:03'" {:type :time, :days 1, :hours 2, :minutes 3}
    "'1 02'" {:type :time, :days 1, :hours 2}
    "'04'" {:type :time, :seconds 4}
    "TRUE" {:type :boolean, :value true}
    "b'1010'" {:type :bitfield, :value "1010"}
    "NULL" {:type :null}))

(deftest columns
  (are [r e] (= r (compile-expression e))
    "C" {:type :column, :column "C"}
    "B.C" {:type :column, :table "B", :column "C"}
    "A.B.C" {:type :column, :schema "A", :table "B", :column "C"}
    "`PURGE`.`KILL`.`TERMINATED`" {:type :column, :schema "PURGE", :table "KILL", :column "TERMINATED"})
  (is "`A`.`B`.`C`" (compile-expression {:type :column, :schema "A", :table "B", :column "C"} {:quote-identifiers? :always})))

(deftest function-calls
  (are [r e] (= r (compile-expression e))
    "NOW()" {:type :function-call, :name "NOW"}
    "A.B()" {:type :function-call, :schema "A", :name "B"}
    "ABS(-17)" {:type :function-call, :name "ABS", :params [{:type :number, :value -17}]}
    "COUNT(*)" {:type :group-function-call, :name "COUNT", :star? true}
    "AVG(C)" {:type :group-function-call, :name "AVG", :expression {:type :column, :column "C"}}
    "AVG(DISTINCT C)" {:type :group-function-call, :name "AVG", :distinct? true, :expression {:type :column, :column "C"}}))

(deftest string-manipulation
  (are [r e] (= r (compile-expression e))
    "CAST(C AS CHAR)" {:type :cast, :expression {:type :column, :column "C"}, :to {:type "CHAR"}}
    "CAST(C AS CHAR(5))" {:type :cast, :expression {:type :column, :column "C"}, :to {:type "CHAR", :length 5}}
    "CAST(C AS DECIMAL(8, 2))" {:type :cast, :expression {:type :column, :column "C"}, :to {:type "DECIMAL", :precision 8, :scale 2}}
    "CONVERT(C USING utf8)" {:type :convert, :expression {:type :column, :column "C"}, :transcoding-name "utf8"}
    "C COLLATE latin1_german2_ci" {:type :collation, :expression {:type :column, :column "C"}, :collation-name "latin1_german2_ci"}
    "BINARY 'a'" {:type :binary, :expression {:type :string, :value "a"}}
    "A || B || C" {:type :concatenation, :expressions [{:type :column, :column "A"}, {:type :column, :column "B"}, {:type :column, :column "C"}]}))

(deftest parameters-variables
  (are [r e] (= r (compile-expression e))
    "?" {:type :parameter}
    "@MYVAR" {:type :variable, :name "MYVAR"}))

(deftest misc
  (are [r e] (= r (compile-expression e))
    "-C" {:type :negation, :expression {:type :column, :column "C"}}
    "EXISTS (SELECT 1)" {:type :exists, :subquery {:type :select, :projections [{:expression {:type :number, :value 1}}]}}
    "NOT EXISTS (SELECT 1)" {:type :exists, :not? true, :subquery {:type :select, :projections [{:expression {:type :number, :value 1}}]}}
    "(SELECT 1)" {:type :subquery, :subquery {:type :select, :projections [{:expression {:type :number, :value 1}}]}}
    "MATCH (A, B) AGAINST ('test')" {:type :match, :columns [{:type :column, :column "A"}, {:type :column, :column "B"}], :against {:type :string, :value "test"}}
    "MATCH (A) AGAINST ('test') IN NATURAL LANGUAGE MODE" {:type :match, :columns [{:type :column, :column "A"}], :against {:type :string, :value "test"}, :search-modifier :in-natural-language-mode}
    "MATCH (A) AGAINST ('test') IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION" {:type :match, :columns [{:type :column, :column "A"}], :against {:type :string, :value "test"}, :search-modifier :in-natural-language-mode-with-query-expansion}
    "MATCH (A) AGAINST ('test') IN BOOLEAN MODE" {:type :match, :columns [{:type :column, :column "A"}], :against {:type :string, :value "test"}, :search-modifier :in-boolean-mode}
    "MATCH (A) AGAINST ('test') WITH QUERY EXPANSION" {:type :match, :columns [{:type :column, :column "A"}], :against {:type :string, :value "test"}, :search-modifier :with-query-expansion}
    "CASE WHEN A = 1 THEN 'a' ELSE 'b' END CASE" {:type :case, :conditionals [{:when {:type :comparison, :operator :equals, :expressions [{:type :column, :column "A"}, {:type :number, :value 1}]}, :then {:type :string, :value "a"}}, {:when :else, :then {:type :string, :value "b"}}]}
    "CASE A WHEN 1 THEN 'a' ELSE 'b' END CASE" {:type :case, :value {:type :column, :column "A"}, :conditionals [{:when {:type :number, :value 1}, :then {:type :string, :value "a"}}, {:when :else, :then {:type :string, :value "b"}}]}
    "INTERVAL 30 DAY" {:type :interval, :expression {:type :number, :value 30}, :unit :days}
    "A + 3" {:type :operation, :operation :addition, :operands [{:type :column, :column "A"}, {:type :number, :value 3}]}
    "A - B - C" {:type :operation, :operation :subtraction, :operands [{:type :column, :column "A"}, {:type :column, :column "B"}, {:type :column, :column "C"}]}
    "(1 + 2) * 3" {:type :operation, :operation :multiplication, :operands [{:type :operation, :operation :addition, :operands [{:type :number, :value 1}, {:type :number, :value 2}]}, {:type :number, :value 3}]}
    "C IN (1, 3, 5)" {:type :in, :expression {:type :column, :column "C"}, :in [{:type :number, :value 1}, {:type :number, :value 3}, {:type :number, :value 5}]}
    "C NOT IN (1, 3, 5)" {:type :in, :not? true, :expression {:type :column, :column "C"}, :in [{:type :number, :value 1}, {:type :number, :value 3}, {:type :number, :value 5}]}
    "C BETWEEN 1 AND 5" {:type :between, :expression {:type :column, :column "C"}, :between [{:type :number, :value 1}, {:type :number, :value 5}]}
    "C NOT BETWEEN 1 AND 5" {:type :between, :not? true, :expression {:type :column, :column "C"}, :between [{:type :number, :value 1}, {:type :number, :value 5}]}
    "C LIKE '%TEST'" {:type :like, :expression {:type :column, :column "C"}, :like {:type :string, :value "%TEST"}}
    "C NOT LIKE '%TEST'" {:type :like, :not? true, :expression {:type :column, :column "C"}, :like {:type :string, :value "%TEST"}}
    "'David_' LIKE 'David|_' ESCAPE '|'" {:type :like, :expression {:type :string, :value "David_"}, :like {:type :string, :value "David|_"}, :escape {:type :string, :value "|"}}
    "C SOUNDS LIKE 'Hello'" {:type :sounds-like, :expression {:type :column, :column "C"}, :sounds-like {:type :string, :value "Hello"}}
    "C REGEXP '.*'" {:type :regexp, :expression {:type :column, :column "C"}, :regexp {:type :string, :value ".*"}}
    "C NOT REGEXP '.*'" {:type :regexp, :not? true, :expression {:type :column, :column "C"}, :regexp {:type :string, :value ".*"}}
    "C IS NULL" {:type :is-null, :expression {:type :column, :column "C"}}
    "C IS NOT NULL" {:type :is-null, :not? true, :expression {:type :column, :column "C"}}
    "C = 'test'" {:type :comparison, :operator :equals, :expressions [{:type :column, :column "C"}, {:type :string, :value "test"}]}
    "C = 1 AND D <> 5" {:type :logical, :operation :and, :expressions [{:type :comparison, :operator :equals, :expressions [{:type :column, :column "C"}, {:type :number, :value 1}]}, {:type :comparison, :operator :not-equals, :expressions [{:type :column, :column "D"}, {:type :number, :value 5}]}]}
    "C = 1 OR D <> 5" {:type :logical, :operation :or, :expressions [{:type :comparison, :operator :equals, :expressions [{:type :column, :column "C"}, {:type :number, :value 1}]}, {:type :comparison, :operator :not-equals, :expressions [{:type :column, :column "D"}, {:type :number, :value 5}]}]}
    "C = 1 XOR D <> 5" {:type :logical, :operation :xor, :expressions [{:type :comparison, :operator :equals, :expressions [{:type :column, :column "C"}, {:type :number, :value 1}]}, {:type :comparison, :operator :not-equals, :expressions [{:type :column, :column "D"}, {:type :number, :value 5}]}]}
    "NOT C > 1" {:type :not, :expression {:type :comparison, :operator :greater-than, :expressions [{:type :column, :column "C"}, {:type :number, :value 1}]}}))

(deftest select-statements
  (are [r s] (= r (compile-statement s))
    "SELECT * FROM A"
      {:type :select
       :projections :*
       :from [{:table "A"}]}
    "SELECT C.* FROM A.B AS C"
      {:type :select
       :projections [{:star {:table "C"}}]
       :from [{:schema "A", :table "B", :alias "C"}]}
    "SELECT t.name, AVG(g.score) AS avg_score FROM team AS t, game AS g WHERE t.id = g.team GROUP BY t.name WITH ROLLUP ORDER BY t.name"
      {:type :select
       :projections [{:expression {:type :column, :table "t", :column "name"}}
                     {:expression {:type :function-call, :name "AVG", :params [{:type :column, :table "g", :column "score"}]}, :alias "avg_score"}]
       :from [{:table "team", :alias "t"}
              {:table "game", :alias "g", :join-type :inner}]
       :where {:type :comparison, :operator :equals, :expressions [{:type :column, :table "t", :column "id"}
                                                                   {:type :column, :table "g", :column "team"}]}
       :group-by [{:type :column, :table "t", :column "name"}]
       :group-with-rollup? true
       :order-by [{:expression {:type :column, :table "t", :column "name"}}]}
    "SELECT HIGH_PRIORITY * FROM atable IGNORE INDEX (idx001) WHERE acol = @avar"
      {:type :select
       :options #{:high-priority}
       :projections :*
       :from [{:table "atable", :index-hints [{:action :ignore, :index-key :index, :indexes ["idx001"]}]}]
       :where {:type :comparison, :operator :equals, :expressions [{:type :column, :column "acol"}
                                                                   {:type :variable, :name "avar"}]}}
    "SELECT CONVERT('abc' USING utf8)"
      {:type :select
       :projections [{:expression {:type :convert, :expression {:type :string, :value "abc"}, :transcoding-name "utf8"}}]}
    "SELECT * FROM atable LIMIT 10 OFFSET 100"
      {:type :select
       :projections :*
       :from [{:table "atable"}]
       :limit 10
       :offset 100}))

(deftest union-statements
  (are [r s] (= r (compile-statement s))
    "SELECT 1 UNION SELECT 2"
      {:type :union
       :selects [{:type :select, :projections [{:expression {:type :number, :value 1}}]}
                 {:type :select, :projections [{:expression {:type :number, :value 2}}]}]}
    "SELECT 1 UNION ALL SELECT 2"
      {:type :union
       :all? true
       :selects [{:type :select, :projections [{:expression {:type :number, :value 1}}]}
                 {:type :select, :projections [{:expression {:type :number, :value 2}}]}]}))

(deftest validations
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Value does not match schema.*" (compile-expression {:type :invalid})))
  (is (thrown-with-msg? java.lang.RuntimeException #"Unknown expression type: :invalid" (compile-expression {:type :invalid} {:validate? false})))
  (is (= (compile-expression {:type :number, :value 1}) "1"))
  (is (= (compile-expression {:type :number, :value 1} {:validate? false}) "1")))