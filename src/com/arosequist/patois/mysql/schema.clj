(ns com.arosequist.patois.mysql.schema
  (:require [schema.core :refer [enum either eq optional-key recursive Number Int Keyword String]]))

(declare SelectStatement)

(def Expression
  (either
    {:type (eq :string)
     :value String
     (optional-key :charset) String}
    {:type (eq :number)
     :value Number}
    {:type (eq :date)
     :year Int
     :month Int
     :date Int
     (optional-key :hour) Int
     (optional-key :minute) Int
     (optional-key :second) Number}
    {:type (eq :time)
     (optional-key :days) Int
     (optional-key :hours) Int
     (optional-key :minutes) Int
     (optional-key :seconds) Number}
    {:type (eq :boolean)
     :value Boolean}
    {:type (eq :bitfield)
     :value String}
    {:type (eq :null)}
    {:type (eq :column)
     :column String
     (optional-key :schema) String
     (optional-key :table) String}
    {:type (eq :function-call)
     :name String
     (optional-key :schema) String
     (optional-key :params) [(recursive #'Expression)]}
    {:type (eq :group-function-call)
     :name String ; enumerated?
     (optional-key :star?) Boolean
     (optional-key :distinct?) Boolean
     (optional-key :expression) (recursive #'Expression)}
    {:type (eq :cast)
     :expression (recursive #'Expression)
     :to {:type String
          (optional-key :length) Int
          (optional-key :precision) Int
          (optional-key :scale) Int}}
    {:type (eq :convert)
     :expression (recursive #'Expression)
     :transcoding-name String}
    {:type (eq :collation)
     :expression (recursive #'Expression)
     :collation-name String}
    {:type (eq :binary)
     :expression (recursive #'Expression)}
    {:type (eq :concatenation)
     :expressions [(recursive #'Expression)]}
    {:type (eq :parameter)}
    {:type (eq :variable)
     :name String}
    {:type (eq :negation)
     :expression (recursive #'Expression)}
    {:type (eq :exists)
     :subquery (recursive #'SelectStatement)
     (optional-key :not?) Boolean}
    {:type (eq :subquery)
     :subquery (recursive #'SelectStatement)}
    {:type (eq :match)
     :columns [(recursive #'Expression)]
     :against (recursive #'Expression)
     (optional-key :search-modifier) Keyword}
    {:type (eq :case)
     :conditionals [{:when (either (recursive #'Expression) (eq :else))
                     :then (recursive #'Expression)}]
     (optional-key :value) (recursive #'Expression)}
    {:type (eq :interval)
     :expression (recursive #'Expression)
     :unit (enum :microseconds :seconds :minutes :hours :days :weeks :months :quarters :years :second-microseconds :minute-microseconds :minute-seconds :hour-microseconds :hour-seconds :hour-minutes :day-microseconds :day-seconds :day-minutes :day-hours :year-months)}
    {:type (eq :operation)
     :operation (enum :bit-or :bit-and :shift-left :shift-right :addition :subtraction :multiplication :division :integer-division :modulus :exponentiation)
     :operands [(recursive #'Expression)]}
    {:type (eq :in)
     :expression (recursive #'Expression)
     :in [(recursive #'Expression)]
     (optional-key :not?) Boolean}
    {:type (eq :between)
     :expression (recursive #'Expression)
     :between [(recursive #'Expression)]
     (optional-key :not?) Boolean}
    {:type (eq :like)
     :expression (recursive #'Expression)
     :like (recursive #'Expression)
     (optional-key :not?) Boolean
     (optional-key :escape) (recursive #'Expression)}
    {:type (eq :sounds-like)
     :expression (recursive #'Expression)
     :sounds-like (recursive #'Expression)}
    {:type (eq :regexp)
     :expression (recursive #'Expression)
     :regexp (recursive #'Expression)
     (optional-key :not?) Boolean}
    {:type (eq :is-null)
     :expression (recursive #'Expression)
     (optional-key :not?) Boolean}
    {:type (eq :comparison)
     :operator (enum :equals :not-equals :less-than :less-than-or-equal-to :greater-than :greather-than-or-equal-to)
     :expressions [(recursive #'Expression)]}
    {:type (eq :logical)
     :operation (enum :and :or :xor)
     :expressions [(recursive #'Expression)]}
    {:type (eq :not)
     :expression (recursive #'Expression)}))

(def OrderByClause
  [{:expression Expression
    (optional-key :asc-desc) (enum :asc :desc)}])

(def SelectClause
  {:projections (either (eq :*)
                        [{(optional-key :expression) Expression
                          (optional-key :star) {:table String
                                                (optional-key :schema) String}
                          (optional-key :alias) String}])
   (optional-key :options) #{(enum :distinct :high-priority :straight-join :sql-small-result :sql-big-result :sql-buffer-result :sql-cache :sql-no-cache :sql-calc-found-rows)}
   (optional-key :from) [{(optional-key :schema) String
                          (optional-key :table) String
                          (optional-key :subquery) SelectStatement
                          (optional-key :natural-join?) Boolean
                          (optional-key :join-type) (enum :inner :straight :left-outer :right-outer)
                          (optional-key :on) Expression
                          (optional-key :using) [String]
                          (optional-key :partitions) [String]
                          (optional-key :alias) String
                          (optional-key :index-hints) [{:action (enum :use :ignore :force)
                                                        :indexes [String]
                                                        (optional-key :for) (enum :join :order-by :group-by)
                                                        (optional-key :index-key) (enum :index :key)}]}]
   (optional-key :where) Expression
   (optional-key :group-by) [Expression]
   (optional-key :group-with-rollup?) Boolean
   (optional-key :having) Expression
   (optional-key :order-by) OrderByClause
   (optional-key :limit) Int
   (optional-key :offset) Int})

(def SelectStatement
  {:selects [{:select SelectClause
              (optional-key :union-all?) Boolean}]
   (optional-key :order-by) OrderByClause
   (optional-key :limit) Int
   (optional-key :offset) Int
   (optional-key :options) #{(enum :for-update :lock-in-shared-mode)}})