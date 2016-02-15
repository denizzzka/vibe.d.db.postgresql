module vibe.db.postgresql.client;

import vibe.db.postgresql.database;

class PostgresClient
{
    private Database db;

    package this(Database database)
    {
        db = database;
    }
}
