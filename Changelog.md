Changelog
=========

1.3.0
-----

### New Features

* Cascaded Processing

    * The concept of a user-defined "stream" was introduced.
      Similar to `CREATE VIEW` in SQL, `CREATE STREAM name FROM SELECT ...`
      allows to create a stream holding the results of a SELECT query over
      some input stream.
    * In particular, `ANALYZE` results can be added to a stream in a new
      column and used further down in the processing pipeline.
    * A user can define custom functions in JavaScript and use them in
      queries using the `CREATE FUNCTION` statement.
    * Multiple data sources can be defined and used one after another
      for updating/analyzing a model.

* Trigger-Based Action

    * A user can also define functions without a return value using
      `CREATE TRIGGER FUNCTION` and attach them as trigger on a stream
      using `CREATE TRIGGER`. This can be used to act based on the contents
      of a stream, in particular analysis results.

* Time-Series Analysis using Sliding Windows

    * To analyze time-series data, sliding windows over an input stream
      (based on either item count or an embedded timestamp) can be computed
      and data in each window aggregated using a set of provided functions
      such as standard deviation or histogram.
    * The results of this aggregation can be used like any other data
      stream.

* Other

    * It is now possible to do feature extraction using user-defined
      functions.

### Breaking Changes

* `CREATE DATASOURCE`

    * A schema should now in general be provided, as in
      many cases schema inference will lead to errors whenever an empty
      data batch is encountered.

* `CREATE MODEL`

    * The syntax for specifying the label/id column has changed from
      `model_name WITH (label: "class", datum: "name")` to
      `model_name (label: class) AS ...`
    * Feature converters are not specified in the JSON configuration any
      more, but instead with a `column WITH converter` syntax.

* `UPDATE MODEL`

    * The statement will only establish the connection between stream and
      model, processing will not start yet. This will be done by
      `START PROCESSING`.

1.2.0
-----

This is the first public release. See the documentation for features and usage information.
