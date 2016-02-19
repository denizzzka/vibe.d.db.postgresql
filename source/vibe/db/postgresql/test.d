module vibe.db.postgresql.test;

version(IntegrationTest)
{
    import std.getopt;
    import std.experimental.logger;

    bool debugEnabled = false;
    string connString;

    void readOpts(string[] args)
    {
        auto helpInformation = getopt(
                args,
                "debug", &debugEnabled,
                "conninfo", &connString
            );
    }

    int main(string[] args)
    {
        readOpts(args);
        if(!debugEnabled)
            sharedLog.logLevel = LogLevel.warning;

        import db = vibe.db.postgresql;
        db.__integration_test(connString);

        return 0;
    }

    shared static this()
    {
        sharedLog.logLevel = LogLevel.trace;
    }
}
