module vibe.db.postgresql;

@trusted:

public import dpq2.result;
public import dpq2.connection: ConnectionException, connStringCheck;
public import dpq2.query: QueryParams;
public import derelict.pq.pq;
import dpq2: ValueFormat, Dpq2Exception, WaitType;
import vibeConnPool = vibe.core.connectionpool;
import vibe.core.concurrency;
import std.experimental.logger;
import core.time: Duration;
import std.exception: enforce;

PostgresClient!TConnection connectPostgresDB(TConnection = dpq2.Connection)(string connString, uint connNum, bool startImmediately = false)
{
    return new PostgresClient!TConnection(connString, connNum, startImmediately);
}

class PostgresClient(TConnection = Connection)
{
    package alias LockedConnection = vibeConnPool.LockedConnection!TConnection;

    private vibeConnPool.ConnectionPool!TConnection pool;
    private const string connString;

    this(string connString, uint connNum, bool startImmediately, TConnection delegate() connFactory = null)
    {
        connString.connStringCheck;
        this.connString = connString;

        pool = new vibeConnPool.ConnectionPool!TConnection(
                (connFactory is null ? &connectionFactory :  connFactory),
                connNum
            );

        if(startImmediately)
        {
            auto conn = new LockedConnection[pool.maxConcurrency];

            foreach(i; 0..pool.maxConcurrency)
                conn[i] = pool.lockConnection();

            foreach(i; 0..pool.maxConcurrency)
                conn[i].destroy();
        }
    }

    private TConnection connectionFactory()
    {
        trace("creating new connection");
        auto c = new TConnection;
        c.connString = connString;
        c.connectStart;
        trace("new connection is started");

        return c;
    }

    LockedConnection lockConnection()
    {
        return pool.lockConnection();
    }

    private void doQuery(void delegate(TConnection) doesQueryAndCollectsResults, bool waitForEstablishConn)
    {
        LockedConnection conn;

        // Try to get usable connection and send SQL command
        try
        {
            trace("get connection from a pool");
            conn = lockConnection();

            while(true) // cycle is need only for polling with waitForEstablishConn
            {
                auto pollRes = conn.poll;
                if(pollRes != PGRES_POLLING_OK)
                {
                    if(!waitForEstablishConn)
                    {
                        trace("connection isn't suitable for query, pollRes=", pollRes, ", conn status=", conn.status);
                        conn.destroy(); // reverts locked connection
                    }
                    else
                    {
                        // waiting for socket changes for reading
                        conn.waitEndOf(WaitType.READ); // FIXME: need timeout check
                        continue;
                    }
                }

                break;
            }

            trace("doesQuery() call");
            doesQueryAndCollectsResults(conn);
            conn.destroy(); // reverts locked connection
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
                conn.disconnect();
                conn.connectStart();
            }
            catch(ConnectionException e)
            {
                warning("Connection restore failed: ", e.msg);
            }

            conn.destroy(); // reverts locked connection
            throw e;
        }
    }

    private immutable(Result) runStatementBlockingManner(void delegate(TConnection) sendsStatement, Duration timeout, bool waitForEstablishConn)
    {
        trace("runStatementBlockingManner");
        immutable(Result)[] res;

        doQuery((conn)
            {
                sendsStatement(conn);
                auto timeoutNotOccurred = conn.waitEndOf(WaitType.READ, timeout);

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
            },
        waitForEstablishConn);

        enforce(res.length == 1, "Result isn't received?");

        return res[0];
    }

    immutable(Answer) execStatement(
        string sqlCommand,
        ValueFormat resultFormat = ValueFormat.TEXT,
        Duration timeout = Duration.zero,
        bool waitForEstablishConn = true
    )
    {
        QueryParams p;
        p.resultFormat = resultFormat;
        p.sqlCommand = sqlCommand;

        return execStatement(p, timeout, waitForEstablishConn);
    }

    immutable(Answer) execStatement(QueryParams params, Duration timeout = Duration.zero, bool waitForEstablishConn = true)
    {
        void dg(TConnection conn)
        {
            conn.sendQuery(params);
        }

        auto res = runStatementBlockingManner(&dg, timeout, waitForEstablishConn);

        return res.getAnswer;
    }

    void prepareStatement(
        string statementName,
        string sqlStatement,
        size_t nParams,
        Duration timeout = Duration.zero,
        bool waitForEstablishConn = true
    )
    {
        auto r = runStatementBlockingManner(
                (conn){conn.sendPrepare(statementName, sqlStatement, nParams);},
                timeout,
                waitForEstablishConn
            );

        if(r.status != PGRES_COMMAND_OK)
            throw new PostgresClientException(r.resultErrorMessage, __FILE__, __LINE__);
    }

    immutable(Answer) execPreparedStatement(QueryParams params, Duration timeout = Duration.zero, bool waitForEstablishConn = true)
    {
        auto res = runStatementBlockingManner((conn){conn.sendQueryPrepared(params);}, timeout, waitForEstablishConn);

        return res.getAnswer;
    }

    string escapeIdentifier(string s)
    {
        auto conn = pool.lockConnection();
        string res = conn.escapeIdentifier(s);
        conn.destroy(); // reverts locked connection

        return res;
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

    {
        auto res1 = client.execStatement(
            "SELECT 123::integer, 567::integer, 'asd fgh'::text",
            ValueFormat.BINARY,
            dur!"seconds"(5)
        );

        assert(res1.getAnswer[0][1].as!PGinteger == 567);
    }

    {
        client.prepareStatement("stmnt_name", "SELECT 123::integer", 0, dur!"seconds"(5));

        bool throwFlag = false;

        try
            client.prepareStatement("wrong_stmnt", "WRONG SQL STATEMENT", 0, dur!"seconds"(5));
        catch(PostgresClientException e)
            throwFlag = true;

        assert(throwFlag);
    }

    {
        QueryParams p;
        p.preparedStatementName = "stmnt_name";

        auto r = client.execPreparedStatement(p, dur!"seconds"(5));

        assert(r.getAnswer[0][0].as!PGinteger == 123);
    }

    {
        assert(client.escapeIdentifier("abc") == "\"abc\"");
    }
}
