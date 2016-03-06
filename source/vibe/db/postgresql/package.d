module vibe.db.postgresql;

@trusted:

public import dpq2.result;
public import dpq2.connection: ConnectionException, connStringCheck, ConnectionStart;
public import dpq2.query: QueryParams;
public import derelict.pq.pq;
import dpq2: ValueFormat, Dpq2Exception, WaitType;
import vibeConnPool = vibe.core.connectionpool;
import vibe.core.concurrency;
import std.experimental.logger;
import core.time: Duration;
import std.exception: enforce;

PostgresClient!TConnection connectPostgresDB(TConnection = dpq2.Connection)(string connString, uint connNum)
{
    TConnection connectionFactory()
    {
        trace("creating new connection");
        auto c = new TConnection(ConnectionStart(), connString);
        trace("new connection is started");

        return c;
    }

    return new PostgresClient!TConnection(connString, connNum, &connectionFactory);
}

class PostgresClient(TConnection = Connection)
{
    private alias VibePool = vibeConnPool.ConnectionPool!TConnection;

    private VibePool pool;
    private const string connString;

    this(string connString, uint connNum, TConnection delegate() connFactory)
    {
        connString.connStringCheck;
        this.connString = connString;

        pool = new VibePool(connFactory, connNum);
    }

    LockedConnection!TConnection lockConnection()
    {
        trace("get connection from a pool");

        return new LockedConnection!TConnection(pool.lockConnection);
    }
}

class LockedConnection(TConnection)
{
    private alias VibeLockedConnection = vibeConnPool.LockedConnection!TConnection;

    VibeLockedConnection conn;
    alias conn this;

    private this(VibeLockedConnection conn)
    {
        this.conn = conn;
    }

    ~this()
    {
        conn.destroy(); // reverts locked connection to a pool
    }

    private void doQuery(void delegate() doesQueryAndCollectsResults)
    {
        // Try to get usable connection and send SQL command
        try
        {
            while(true)
            {
                auto pollRes = conn.poll;

                if(pollRes != PGRES_POLLING_OK)
                {
                    // waiting for socket changes for reading
                    conn.waitEndOf(WaitType.READ); // FIXME: need timeout check
                    continue;
                }

                break;
            }

            trace("doesQuery() call");
            doesQueryAndCollectsResults();
            return;
        }
        catch(ConnectionException e)
        {
            // this block just starts reconnection and immediately loops back
            warning("Connection failed: ", e.msg);

            assert(conn, "conn isn't initialised (conn == null)");

            // try to restore connection because pool isn't do this job by itself
            try
            {
                trace("try to restore not null connection");
                conn.resetStart();
            }
            catch(ConnectionException e)
            {
                warning("Connection restore failed: ", e.msg);
            }

            throw e;
        }
    }

    private immutable(Result) runStatementBlockingManner(void delegate() sendsStatement, Duration timeout)
    {
        trace("runStatementBlockingManner");
        immutable(Result)[] res;

        doQuery(()
            {
                sendsStatement();
                bool timeoutNotOccurred = conn.waitEndOf(WaitType.READ, timeout);

                if(!timeoutNotOccurred) // query timeout occurred
                {
                    trace("Exceeded Posgres query time limit");
                    conn.cancel(); // cancel sql query
                }

                trace("consumeInput()");
                conn.consumeInput();

                while(true)
                {
                    trace("getResult()");
                    auto r = conn.getResult();
                    if(r is null) break;
                    res ~= r;
                }

                enforce(res.length <= 1, "simple query can return only one Result instance");

                if(!timeoutNotOccurred && res.length != 1) // query timeout occured and result isn't received
                    throw new PostgresClientException("Exceeded Posgres query time limit", __FILE__, __LINE__);
            }
        );

        enforce(res.length == 1, "Result isn't received?");

        return res[0];
    }

    immutable(Answer) execStatement(
        string sqlCommand,
        ValueFormat resultFormat = ValueFormat.TEXT,
        Duration timeout = Duration.zero
    )
    {
        QueryParams p;
        p.resultFormat = resultFormat;
        p.sqlCommand = sqlCommand;

        return execStatement(p, timeout);
    }

    immutable(Answer) execStatement(QueryParams params, Duration timeout = Duration.zero)
    {
        auto res = runStatementBlockingManner({ conn.sendQuery(params); }, timeout);

        return res.getAnswer;
    }

    /// currently unusable due to connections stored in the pool
    void prepareStatement_unusableMethod(
        string statementName,
        string sqlStatement,
        size_t nParams,
        Duration timeout = Duration.zero
    )
    {
        auto r = runStatementBlockingManner(
                {conn.sendPrepare(statementName, sqlStatement, nParams);},
                timeout
            );

        if(r.status != PGRES_COMMAND_OK)
            throw new PostgresClientException(r.resultErrorMessage, __FILE__, __LINE__);
    }

    immutable(Answer) execPreparedStatement(QueryParams params, Duration timeout = Duration.zero)
    {
        auto res = runStatementBlockingManner({ conn.sendQueryPrepared(params); }, timeout);

        return res.getAnswer;
    }
}

class PostgresClientException : Dpq2Exception
{
    this(string msg, string file, size_t line)
    {
        super(msg, file, line);
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
        auto res1 = conn.execStatement(
            "SELECT 123::integer, 567::integer, 'asd fgh'::text",
            ValueFormat.BINARY,
            dur!"seconds"(5)
        );

        assert(res1.getAnswer[0][1].as!PGinteger == 567);
    }

    {
        conn.prepareStatement_unusableMethod("stmnt_name", "SELECT 123::integer", 0, dur!"seconds"(5));

        bool throwFlag = false;

        try
            conn.prepareStatement_unusableMethod("wrong_stmnt", "WRONG SQL STATEMENT", 0, dur!"seconds"(5));
        catch(PostgresClientException e)
            throwFlag = true;

        assert(throwFlag);
    }

    {
        QueryParams p;
        p.preparedStatementName = "stmnt_name";

        auto r = conn.execPreparedStatement(p, dur!"seconds"(5));

        assert(r.getAnswer[0][0].as!PGinteger == 123);
    }

    {
        assert(conn.escapeIdentifier("abc") == "\"abc\"");
    }
}
