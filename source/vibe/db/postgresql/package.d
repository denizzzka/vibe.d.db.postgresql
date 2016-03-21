module vibe.db.postgresql;

public import dpq2.result;
public import dpq2.connection: ConnectionException, connStringCheck, ConnectionStart;
public import dpq2.args;
public import derelict.pq.pq;

import dpq2: ValueFormat, Dpq2Exception;
import vibe.db.postgresql.pool;
import vibe.core.log;
import core.time: Duration;
import std.exception: enforce;

PostgresClient connectPostgresDB(string connString, uint connNum)
{
    return new PostgresClient(connString, connNum);
}

private struct ClientSettings
{
    string connString;
    void delegate(Connection) afterStartConnectOrReset;
}

class PostgresClient
{
    private shared ConnectionPool!Connection pool;
    private immutable ClientSettings settings;

    this(
        string connString,
        uint connNum,
        void delegate(Connection) afterStartConnectOrReset = null
    )
    {
        connString.connStringCheck;

        settings = ClientSettings(
            connString,
            afterStartConnectOrReset
        );

        pool = new ConnectionPool!Connection({ return new Connection(settings); }, connNum);
    }

    shared this(
        string connString,
        uint connNum,
        void delegate(Connection) afterStartConnectOrReset = null
    )
    {
        enforce(PQisthreadsafe() == 1);
        connString.connStringCheck;

        settings = ClientSettings(
            connString,
            afterStartConnectOrReset
        );

        pool = new ConnectionPool!Connection({ return new Connection(settings); }, connNum);
    }

    LockedConnection!Connection lockConnection()
    {
        logDebugV("get connection from a pool");

        return pool.lockConnection();
    }

    synchronized LockedConnection!Connection lockConnection() shared
    {
        logDebugV("get connection from a shared pool");

        return pool.lockConnection();
    }
}

class Connection : dpq2.Connection
{
    Duration socketTimeout = dur!"seconds"(10);
    Duration statementTimeout = dur!"seconds"(30);

    private const ClientSettings* settings;

    private this(const ref ClientSettings settings)
    {
        this.settings = &settings;

        super(settings.connString);
        setClientEncoding("UTF8"); // TODO: do only if it is different from UTF8

        import std.conv: to;
        logDebugV("creating new connection, delegate isNull="~(settings.afterStartConnectOrReset is null).to!string);

        if(settings.afterStartConnectOrReset !is null)
            settings.afterStartConnectOrReset(this);
    }

    override void resetStart()
    {
        super.resetStart;

        if(settings.afterStartConnectOrReset !is null)
            settings.afterStartConnectOrReset(this);
    }

    private void waitEndOfRead(in Duration timeout) // TODO: rename to waitEndOf + add FileDescriptorEvent.Trigger argument
    {
        import vibe.core.core;

        auto sock = this.socket();

        sock.blocking = false;
        scope(exit) sock.blocking = true;

        auto event = createFileDescriptorEvent(sock.handle, FileDescriptorEvent.Trigger.any);

        if(!event.wait(timeout))
            throw new PostgresClientTimeoutException(__FILE__, __LINE__);
    }

    private void doQuery(void delegate() doesQueryAndCollectsResults)
    {
        // Try to get usable connection and send SQL command
        try
        {
            while(true)
            {
                auto pollRes = poll();

                if(pollRes != PGRES_POLLING_OK)
                {
                    // waiting for socket changes for reading
                    waitEndOfRead(socketTimeout);

                    continue;
                }

                break;
            }

            logDebugV("doesQuery() call");
            doesQueryAndCollectsResults();
        }
        catch(ConnectionException e)
        {
            // this block just starts reconnection and immediately loops back
            tryResetConnection(e);
            throw e;
        }
        catch(PostgresClientTimeoutException e)
        {
            tryResetConnection(e);
            throw e;
        }
    }

    private void tryResetConnection(Exception e)
    {
            logWarn("Connection failed: "~e.msg);

            assert(conn, "conn isn't initialised (conn == null)");

            // try to restore connection because pool isn't do this job by itself
            try
            {
                logDebugV("try to restore not null connection");
                resetStart();
            }
            catch(ConnectionException e)
            {
                logWarn("Connection restore failed: ", e.msg);
            }
    }

    private immutable(Result) runStatementBlockingManner(void delegate() sendsStatement)
    {
        logDebugV("runStatementBlockingManner");
        immutable(Result)[] res;

        doQuery(()
            {
                sendsStatement();

                try
                {
                    waitEndOfRead(statementTimeout);
                }
                catch(PostgresClientTimeoutException e)
                {
                    logDebugV("Exceeded Posgres query time limit");
                    cancel(); // cancel sql query
                    throw e;
                }
                finally
                {
                    logDebugV("consumeInput()");
                    consumeInput();

                    while(true)
                    {
                        logDebugV("getResult()");
                        auto r = getResult();
                        if(r is null) break;
                        res ~= r;
                    }

                    enforce(res.length <= 1, "simple query can return only one Result instance");
                }
            }
        );

        enforce(res.length == 1, "Result isn't received?");

        return res[0];
    }

    immutable(Answer) execStatement(
        string sqlCommand,
        ValueFormat resultFormat = ValueFormat.TEXT
    )
    {
        QueryParams p;
        p.resultFormat = resultFormat;
        p.sqlCommand = sqlCommand;

        return execStatement(p);
    }

    immutable(Answer) execStatement(in ref QueryParams params)
    {
        auto res = runStatementBlockingManner({ sendQueryParams(params); });

        return res.getAnswer;
    }

    void prepareStatement(
        string statementName,
        string sqlStatement
    )
    {
        auto r = runStatementBlockingManner(
                {sendPrepare(statementName, sqlStatement);}
            );

        if(r.status != PGRES_COMMAND_OK)
            throw new PostgresClientException(r.resultErrorMessage, __FILE__, __LINE__);
    }

    immutable(Answer) execPreparedStatement(in ref QueryParams params)
    {
        auto res = runStatementBlockingManner({ sendQueryPrepared(params); });

        return res.getAnswer;
    }
}

class PostgresClientException : Dpq2Exception // TODO: remove it (use dpq2 exception)
{
    this(string msg, string file, size_t line)
    {
        super(msg, file, line);
    }
}

class PostgresClientTimeoutException : Dpq2Exception
{
    this(string file, size_t line)
    {
        super("Exceeded Posgres query time limit", file, line);
    }
}

unittest
{
    bool raised = false;

    try
    {
        auto client = connectPostgresDB("wrong connect string", 2);
    }
    catch(ConnectionException e)
        raised = true;

    assert(raised);
}

version(IntegrationTest) void __integration_test(string connString)
{
    setLogLevel = LogLevel.debugV;

    auto client = new shared PostgresClient(connString, 3);
    auto conn = client.lockConnection();

    {
        auto res = conn.execStatement(
            "SELECT 123::integer, 567::integer, 'asd fgh'::text",
            ValueFormat.BINARY
        );

        assert(res.getAnswer[0][1].as!PGinteger == 567);
    }

    {
        conn.prepareStatement("stmnt_name", "SELECT 123::integer");

        bool throwFlag = false;

        try
            conn.prepareStatement("wrong_stmnt", "WRONG SQL STATEMENT");
        catch(PostgresClientException e)
            throwFlag = true;

        assert(throwFlag);
    }

    {
        QueryParams p;
        p.preparedStatementName = "stmnt_name";

        auto r = conn.execPreparedStatement(p);

        assert(r.getAnswer[0][0].as!PGinteger == 123);
    }

    {
        // Fibers test
        import vibe.core.concurrency;

        auto future0 = async({
            auto conn = client.lockConnection;
            immutable answer = conn.execStatement("SELECT 'New connection 0'");
            return 1;
        });

        auto future1 = async({
            auto conn = client.lockConnection;
            immutable answer = conn.execStatement("SELECT 'New connection 1'");
            return 1;
        });

        immutable answer = conn.execStatement("SELECT 'Old connection'");

        assert(future0 == 1);
        assert(future1 == 1);
        assert(answer.length == 1);
    }

    {
        assert(conn.escapeIdentifier("abc") == "\"abc\"");
    }
}
