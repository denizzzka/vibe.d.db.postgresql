import std.getopt;
import std.experimental.logger;

shared static this()
{
    sharedLog.fatalHandler = null;
}

string configFileName = "/wrong/path/to/file.json";
bool debugEnabled = false;

void readOpts(string[] args)
{
    try
    {
        auto helpInformation = getopt(
                args,
                "debug", &debugEnabled,
                "config", &configFileName
            );
    }
    catch(Exception e)
    {
        fatal(e.msg);
    }

    if(!debugEnabled) sharedLog.logLevel = LogLevel.warning;
}

import vibe.data.bson;

private Bson _cfg;

void readConfig()
{
    import std.file;

    try
    {
        auto text = readText(configFileName);
        _cfg = Bson(text);
    }
    catch(Exception e)
    {
        fatal(e.msg);
    }
}

int main(string[] args)
{
    readOpts(args);
    readConfig();

    return 0;
}
