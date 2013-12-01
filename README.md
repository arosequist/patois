patois
======

A Clojure library providing small, explicit interfaces for dialect-specific SQL statements.

## Seriously, another SQL library?

I know, and I'm sorry. Think of this less as a library and more as an experiment. I haven't seen this same approach taken elsewhere, so maybe it's worth exploring.

[Documentation](http://arosequist.github.io/patois/)

patois wants to treat SQL statements as normal Clojure data structures instead of strings, to enable us to compose them, analyze them, and build more complicated abstractions on top of them.

Doing this usually limits what we can express in our statements, though. To use vendor-specific features, we have to go back to strings.

patois attempts to reconcile these by providing different schemas and compilation functions for each DBMS rather than a one-size-fits-all data structure. This allows them to be simpler and more comprehensive, which should eliminate the need to ever resort to manual SQL string manipulation.

## Status

This is still in early development. Don't use it if you aren't planning on working on it.

## License

Copyright © 2013 Anthony Rosequist

Distributed under the Eclipse Public License.
