module vibe.db.postgresql.example;

import vibe.d;
import vibe.db.postgresql;

PostgresClient client;

void test()
{
    client.pickConnection(
        (scope conn)
        {
            immutable result = conn.exec(
                "SELECT 123 as first_num, 567 as second_num, 'abc'::text as third_text "~
                "UNION ALL "~
                "SELECT 890, 233, 'fgh'::text as third_text",
                ValueFormat.BINARY
            );

            assert(result[0]["second_num"].as!PGinteger == 567);
            assert(result[1]["third_text"].as!PGtext == "fgh");

            foreach (val; rangify(result[0]))
                logInfo("Found entry: %s", val.as!Bson.toJson);
        }
    );
}

void main()
{
    // params: conninfo string, maximum number of connections in
    // the connection pool
    client = new PostgresClient("dbname=postgres user=postgres", 4);

    test();

    logInfo("Done!");
}
