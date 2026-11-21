# JMEOS generators, vendored

`codegen_jvm.py` is the file of [MobilityDB/JMEOS](https://github.com/MobilityDB/JMEOS)
`tools/`, unchanged, at commit `da6409b53e`. It generates the SQL surfaces of MobilityFlink and
MobilitySpark from the MEOS catalog.

`../codegen_nebula_sql.py` reads the catalog through its `SqlModel`, so a MEOS SQL type has in
MobilityNebula the C type, the codec and the carried form it has in Flink and Spark. To update
it, copy the file from a JMEOS commit and name that commit here; it is never edited in
this repository.
