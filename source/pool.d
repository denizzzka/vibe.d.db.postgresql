module pgator2.pool;

import dpq2;
import vibe = vibe.core.connectionpool;
import std.experimental.logger;
import core.time: Duration;
import std.exception: enforce;

alias LockedConnection = vibe.LockedConnection!Connection;

class ConnectionPool
{
    private vibe.ConnectionPool!Connection pool;
    private const string connString;

    // TODO: it would be nice if connNum possible to set up automatically by number of CPU kernels on Postgres side
    this(string connString, uint connNum)
    {
        this.connString = connString;

        pool = new vibe.ConnectionPool!Connection(&connectionFactory, connNum);
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

    private void doQuery(void delegate(Connection) doesQueryAndCollectsResults, bool waitForEstablishConn = false)
    {
        LockedConnection conn;

        // Try to get usable connection and send SQL command
        // Doesn't make sense to do MUCH more attempts to pick a connection than it available
        pick_conn:
        foreach(unused_var; 0..(pool.maxConcurrency * 2))
        {
            try
            {
                trace("get connection from a pool");
                conn = pool.lockConnection();

                while(true) // need only for polling with waitForEstablishConn
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
                            continue pick_conn; // need try other connection
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
                return;
            }
            catch(ConnectionException e)
            {
                // this block just starts reconnection and immediately loops back
                warning("Connection failed: ", e.msg);

                // try to restore connection because pool isn't do this job by itself
                try
                {
                    conn.disconnect();
                    conn.connectStart();
                }
                catch(ConnectionException e)
                {
                    warning("Connection restore failed: ", e.msg);
                }

                conn.destroy(); // reverts locked connection
                continue;
            }
        }

        throw new PoolException("All connections to the Postgres server aren't suitable for query", __FILE__, __LINE__);
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

class PoolException : Exception
{
    this(string msg, string file, size_t line)
    {
        super(msg, file, line);
    }
}

unittest
{
    auto pool = new ConnectionPool("wrong connect string", 2);

    {
        bool raised = false;

        try
        {
            auto c = pool.lockConnection;
            c.exec("SELECT 123");
        }
        catch(ConnectionException e)
            raised = true;

        assert(raised);
    }
}

private immutable(Result) doSimpleSqlCmd(ConnectionPool pool, string sqlCommand, Duration timeout = Duration.zero, bool waitForEstablishConn = false)
{
    immutable(Result)[] res;

    void dg(Connection conn)
    {
        conn.sendQuery(sqlCommand);
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
            throw new PoolException("Exceeded Posgres query time limit", __FILE__, __LINE__);

        enforce(res.length == 1, "query isn't received?");
    }

    pool.doQuery(&dg, waitForEstablishConn);

    return res[0];
}

version(IntegrationTest) void __integration_test(string connString)
{
    auto pool = new ConnectionPool(connString, 3);

    {
        auto res1 = pool.doSimpleSqlCmd("SELECT 123, 567, 'asd fgh'", Duration.zero, true);

        import std.stdio;
        writeln("res1=", res1.getAnswer);

        auto res2 = pool.doSimpleSqlCmd("SELECT 12366666", Duration.zero, true);

        writeln("res2=", res2.getAnswer);
    }
}
