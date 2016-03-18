module vibe.db.postgresql;

@trusted:

public import dpq2.result;
public import dpq2.connection: ConnectionException, connStringCheck, ConnectionStart;
public import dpq2.args;
public import derelict.pq.pq;

import dpq2: ValueFormat, Dpq2Exception;
import vibeConnPool = vibe.core.connectionpool;
import vibe.core.log;
import core.time: Duration;
import std.exception: enforce;

shared (PostgresClient) connectPostgresDB(string connString, uint connNum)
{
    return new shared PostgresClient(connString, connNum);
}

shared class PostgresClient
{
    private alias VibePool = vibeConnPool.ConnectionPool!Connection;

    private const string connString;
    private const void delegate(Connection) afterStartConnectOrReset;
    private VibePool pool;

    this(
        string connString,
        uint connNum,
        void delegate(Connection) @trusted afterStartConnectOrReset = null
    )
    {
        connString.connStringCheck;

        this.connString = connString;
        this.afterStartConnectOrReset = afterStartConnectOrReset;

        pool = cast(shared) new VibePool({ return new Connection(this); }, connNum);
    }

    synchronized vibeConnPool.LockedConnection!Connection lockConnection()
    {
        logDebugV("get connection from a pool");
        return (cast() pool).lockConnection();
    }
}

class Connection : dpq2.Connection
{
    private shared PostgresClient client;
    Duration socketTimeout = dur!"seconds"(10);
    Duration statementTimeout = dur!"seconds"(30);

    private this(shared PostgresClient client)
    {
        super(client.connString);
        this.setClientEncoding("UTF8");

        if(client.afterStartConnectOrReset !is null)
            client.afterStartConnectOrReset(this);
    }

    override void resetStart()
    {
        super.resetStart;

        if(client.afterStartConnectOrReset !is null)
            client.afterStartConnectOrReset(this);
    }

    private void waitEndOfRead(in Duration timeout)
    {
        import vibe.core.core;

        auto sock = this.posixSocket();
        auto event = createFileDescriptorEvent(sock, FileDescriptorEvent.Trigger.read);

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
            logWarn("Connection failed: ", e.msg);

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

    immutable(Answer) execStatement(ref QueryParams params)
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

    immutable(Answer) execPreparedStatement(ref QueryParams params)
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
    auto client = connectPostgresDB(connString, 3);
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
        assert(conn.escapeIdentifier("abc") == "\"abc\"");
    }
}
