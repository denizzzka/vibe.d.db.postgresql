module vibe.db.postgresql;

public import dpq2.answer;
import dpq2;
import vibe = vibe.core.connectionpool;
import std.experimental.logger;
import core.time: Duration;
import std.exception: enforce;

PostgresClient connectPostgresDB(string connString, uint connNum, bool startImmediately = false)
{
    return new PostgresClient(connString,connNum, startImmediately);
}

package alias LockedConnection = vibe.LockedConnection!Connection;

class PostgresClient
{
    private vibe.ConnectionPool!Connection pool;
    private const string connString;

    private this(string connString, uint connNum, bool startImmediately)
    {
        this.connString = connString;

        pool = new vibe.ConnectionPool!Connection(&connectionFactory, connNum);

        if(startImmediately)
        {
            auto conn = new LockedConnection[pool.maxConcurrency];

            foreach(i; 0..pool.maxConcurrency)
                conn[i] = pool.lockConnection();

            foreach(i; 0..pool.maxConcurrency)
                conn[i].destroy();
        }
    }

    private Connection connectionFactory()
    {
        trace("creating new connection");
        auto c = new Connection;
        c.connString = connString;
        c.connectStart;
        trace("new connection is started");

        return c;
    }

    private void doQuery(void delegate(Connection) doesQueryAndCollectsResults, bool waitForEstablishConn)
    {
        LockedConnection conn;

        // Try to get usable connection and send SQL command
        try
        {
            trace("get connection from a pool");
            conn = pool.lockConnection();

            while(true) // cycle is need only for polling with waitForEstablishConn
            {
                if(conn.status == CONNECTION_BAD) // need to reconnect this connection
                    throw new ConnectionException(conn.__conn, __FILE__, __LINE__);

                auto pollRes = conn.poll;
                if(pollRes != CONNECTION_MADE)
                {
                    if(!waitForEstablishConn)
                    {
                        trace("connection isn't suitable for query, pollRes=", pollRes, ", conn status=", conn.status);
                        conn.destroy(); // reverts locked connection
                    }
                    else
                    {
                        // waiting for socket changes for reading
                        conn.waitForReading;
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

            // try to restore connection because pool isn't do this job by itself
            try
            {
                if(conn is null)
                {
                    trace("conn isn't initialised (conn == null)");
                }
                else
                {
                    trace("try to restore not null connection");
                    conn.disconnect();
                    conn.connectStart();
                }
            }
            catch(ConnectionException e)
            {
                warning("Connection restore failed: ", e.msg);
            }

            conn.destroy(); // reverts locked connection
        }

        throw new PostgresClientException("All connections to the Postgres server aren't suitable for query", __FILE__, __LINE__);
    }

    immutable(Answer) execCommand(string sqlCommand, Duration timeout = Duration.zero, bool waitForEstablishConn = true)
    {
        QueryParams p;
        p.resultFormat = ValueFormat.BINARY;
        p.sqlCommand = sqlCommand;

        return execCommand(p, timeout, waitForEstablishConn);
    }

    immutable(Answer) execCommand(QueryParams params, Duration timeout = Duration.zero, bool waitForEstablishConn = true)
    {
        immutable(Result)[] res;

        void dg(Connection conn)
        {
            conn.sendQuery(params);
            auto sockNum = conn.waitForReading(timeout);

            if(sockNum == 0) // query timeout occured
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

            if(sockNum == 0 && res.length != 1) // query timeout occured and result isn't received
                throw new PostgresClientException("Exceeded Posgres query time limit", __FILE__, __LINE__);

            enforce(res.length == 1, "Result isn't received?");
        }

        doQuery(&dg, waitForEstablishConn);

        return res[0].getAnswer;
    }
}

package size_t waitForReading(Connection conn, Duration timeout = Duration.zero)
{
    import std.socket;

    auto socket = conn.socket;
    auto set = new SocketSet;
    set.add(socket);

    trace("waiting for socket changes for reading");
    auto sockNum = Socket.select(set, null, set, timeout);

    enforce(sockNum >= 0);

    return sockNum;
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
    auto client = connectPostgresDB("wrong connect string", 2);

    {
        bool raised = false;

        try
        {
            client.execCommand("SELECT 123");
        }
        catch(PostgresClientException e)
            raised = true;

        assert(raised);
    }
}

version(IntegrationTest) void __integration_test(string connString)
{
    auto client = connectPostgresDB(connString, 3);

    {
        auto res1 = client.execCommand("SELECT 123::integer, 567::integer, 'asd fgh'::text", dur!"seconds"(5));

        assert(res1.getAnswer[0][1].as!PGinteger == 567);
    }
}
