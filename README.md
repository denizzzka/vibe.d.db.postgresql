PostgreSQL support for Vibe.d
====

Example:
```D
import vibe.db.postgresql;
import std.stdio: writefln;

PostgresClient client;

static this()
{
	auto result = client.execCommand(
		"SELECT 123 as first_num, 567 as second_num, 'abc'::text as third_text "~
		"UNION ALL "~
		"SELECT 890, 233, 'fgh'::text as third_text",
	);
	assert(result[0]["second_num"].as!PGinteger == 567);
	assert(result[1]["third_text"].as!PGtext == "fgh");

	foreach (val; rangify(result[0]))
		writefln("Found entry: %s", val.toBson.toJson);
}

shared static this()
{
	// params: conninfo string, number of simultaneous connections
	client = connectPostgresDB("dbname=vibe-test user=postgres", 4);
}
```

Output:
```
Found entry: 123
Found entry: 567
Found entry: "abc"
```
