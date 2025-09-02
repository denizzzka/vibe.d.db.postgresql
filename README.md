PostgreSQL support for Vibe.d

[API documentation](https://denizzzka.github.io/vibe.d.db.postgresql/)

_Please help us to make documentation better!_

====

Example:
```D
module vibe.db.postgresql.example;

import std.getopt;
import vibe.d;
import vibe.db.postgresql;

PostgresClient client;

void performDbRequest()
{
    immutable result = client.pickConnection(
        (scope conn)
        {
            return conn.exec(
                "SELECT 123 as first_num, 567 as second_num, 'abc'::text as third_text "~
                "UNION ALL "~
                "SELECT 890, 233, 'fgh'::text as third_text",
                ValueFormat.BINARY
            );
        }
    );

    assert(result[0]["second_num"].as!PGinteger == 567);
    assert(result[1]["third_text"].as!PGtext == "fgh");

    foreach (val; rangify(result[0]))
        logInfo("Found entry: %s", val.as!Bson.toJson);
}

void main(string[] args)
{
    string connString;
    getopt(args, "conninfo", &connString);

    // params: conninfo string, maximum number of connections in
    // the connection pool
    client = new PostgresClient(connString, 4);

    // This function can be invoked in parallel from different Vibe.d processes
    performDbRequest();

    logInfo("Done!");
}
```

Output:
```
[main(----) INF] Found entry: 123
[main(----) INF] Found entry: 567
[main(----) INF] Found entry: "abc"
[main(----) INF] Done!
```
