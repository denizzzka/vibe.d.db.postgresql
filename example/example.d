module vibe.db.postgresql.example;

import std.getopt;
import vibe.d;
import vibe.db.postgresql;

PostgresClient client;

void performDbRequest()
{
    immutable result = client.pickConnection(
        (scope LockedConnection conn)
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

    void initConnectionDg(Connection conn)
    {
        // D uses UTF8, but Postgres settings may differ. If you want to
        // use text strings it is recommended to force set up UTF8 encoding
        conn.exec(`set client_encoding to 'UTF8'`);

        // Canceling a statement execution due to a timeout implies
        // re-initialization of the connection. Therefore, it is
        // recommended to additionally set a smaller statement
        // execution time limit on the server side so that server can
        // quickly interrupt statement processing on its own
        // initiative without connection re-initialization.
        conn.exec(`set statement_timeout to '15 s'`);
    }

    // params: conninfo string, maximum number of connections in
    // the connection pool and connection initialization delegate
    client = new PostgresClient(connString, 4, &initConnectionDg);

    // This function can be invoked in parallel from different Vibe.d processes or threads
    performDbRequest();

    logInfo("Done!");
}
