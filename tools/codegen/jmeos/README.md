# JMEOS generators, vendored

`codegen_jvm.py` and `codegen_spark_udfs.py` are the files of
[MobilityDB/JMEOS](https://github.com/MobilityDB/JMEOS) `tools/`, unchanged, at commit
`77749f3274`. They generate the SQL surfaces of MobilityFlink and MobilitySpark from the MEOS
catalog.

`../codegen_nebula_sql.py` reads the catalog through their `SqlModel`, so a MEOS SQL type has in
MobilityNebula the C type, the codec and the carried form it has in Flink and Spark. To update
them, copy both files from a JMEOS commit and name that commit here; they are never edited in
this repository.
