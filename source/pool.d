module pgator2.pool;

import dpq2;
import vibe = vibe.core.connectionpool;
import std.experimental.logger;

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

    private void makeTransaction(void delegate(Connection) doesQuery, bool waitForEstablishConn = false)
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
                doesQuery(conn);
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

package void waitForReading(Connection conn)
{
    import std.socket;
    import std.exception: enforce;

    auto socket = conn.socket;
    auto set = new SocketSet;
    set.add(socket);

    trace("waiting for socket changes for reading");
    auto sockNum = Socket.select(set, null, set);

    enforce(sockNum > 0);
}

class PoolException : Exception
{
    this(string msg, string file, size_t line)
    {
        super(msg, file, line);
    }
}

struct TransactionArgs
{
    string sqlCommand;
    string[] sqlArgs;
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

version(IntegrationTest) void __integration_test(string connString)
{
    auto pool = new ConnectionPool(connString, 3);

    {
        TransactionArgs args;
        args.sqlCommand = "SELECT 123";

        immutable(Result)[] res;

        void doQuery(Connection conn)
        {
            conn.sendQuery("SELECT 123, 567, 'asd fgh'");

            auto sock = conn.socket();

            import std.socket;
            import core.time;

            auto readSet = new SocketSet;
            auto errSet = new SocketSet;
            readSet.add(sock);
            errSet.add(sock);

            trace("waiting for data on the socket");
            auto sockNum = Socket.select(readSet, null, errSet);//, dur!"seconds"(10));

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

            if(sockNum == 0) // query timeout occured
                throw new PoolException("Exceeded Posgres query time limit", __FILE__, __LINE__);
        }

        pool.makeTransaction(&doQuery, true);

        import std.stdio;
        writeln("results=", res);

        foreach(r; res)
        {
            import std.stdio;
            writeln("res=", r.getAnswer);
        }

        res.length = 0;
        pool.makeTransaction(&doQuery, true);

        foreach(r; res)
        {
            import std.stdio;
            writeln("res=", r.getAnswer);
        }
    }
}
