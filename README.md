patois
======

A Clojure library providing small, explicit interfaces for dialect-specific SQL statements.

## Seriously, another SQL library?

Yeah, sorry. This is an experiment to try an approach that I haven't seen taken elsewhere.

## Rationale

patois wants to treat SQL statements as normal Clojure data structures instead of strings, to enable us to compose them, analyze them, and build more complicated abstractions on top of them.

Doing this usually limits what we can express in our statements, though. To use vendor-specific features, we have to resort to using strings again.

patois attempts to reconcile these by providing different schemas and compilation functions for each DBMS rather than a one-size-fits-all data structure. This allows them to be simpler and more comprehensive, which should eliminate the need to ever resort to manual SQL string manipulation.

## Example

```clojure
(def a {:projections :*
        :from [{:table "ORDERS", :alias "O"}]
        :order-by [{:type :column, :table "O", :column "ID"}]
        :limit 10
        :offset 50})

; MySQL query
(mysql/compile-select-statement a)
 > "SELECT * FROM ORDERS O ORDER BY O.ID LIMIT 10 OFFSET 50"

; Slightly different syntax
(sql-server/compile-select-statement a)
 > "SELECT * FROM ORDERS O ORDER BY O.ID OFFSET 50 ROWS FETCH NEXT 10 ROWS ONLY"
 
; Offset not supported in DB2, so this is an invalid select statement map
(db2/compile-select-statement a)
 > clojure.lang.ExceptionInfo: Value does not match schema...
 
 
 
; The structures are very explicit and should encompass all features of the specific DBMS used
(mysql/compile-select-statement {:options #{:high-priority}
                                 :projections [{:expression {:type :bitfield, :value "1010"}}
                                               {:expression {:type :collation, :expression {:type :column, :column "LAST_NAME"}, :collation-name "latin1_german2_ci"}}]
                                 :from [{:schema "MYDB", :table "REFERENCES", :index-hints [{:action :ignore, :index-key :index, :indexes ["IDX025"]}]}]
                                 :where {:type :sounds-like, :expression {:type :column, :column "FIRST_NAME"}, :sounds-like {:type :variable, :name "NAME"}}})
 > "SELECT HIGH_PRIORITY b'1010', LAST_NAME COLLATE latin1_german2_ci FROM MYDB.`REFERENCES` IGNORE INDEX (IDX025) WHERE FIRST_NAME SOUNDS LIKE @NAME"
```

## Documentation

Check out the [wiki](https://github.com/arosequist/patois/wiki/)

## Status

This is still in early development. Don't use it if you aren't planning on working on it.

## License

Copyright © 2013 Anthony Rosequist

Distributed under the Eclipse Public License.
