module pgator2.pool;

import dpq2;
import vibe = vibe.core.connectionpool;
import std.experimental.logger;
import std.socket;

alias LockedConnection = vibe.LockedConnection!Connection;

class ConnectionPool : vibe.ConnectionPool!(Connection)
{
    private const string connString;

    this(string connString, uint connNum)
    {
        this.connString = connString;

        super(&connectionFactory, connNum);
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

    immutable(Result)[] makeTransaction(TransactionArgs argsm, bool waitForEstablishConn = false)
    {
        LockedConnection conn;

        // Try to get usable connection and send SQL command
        // Doesn't make sense to do MUCH more attempts to pick a connection than it available
        pick_conn:
        foreach(unused_var; 0..(maxConcurrency * 2))
        {
            try
            {
                trace("get connection from a pool");
                conn = lockConnection();

                while(true)
                {
                    if(conn.status == CONNECTION_BAD) // need to reconnect this connection
                        throw new ConnectionException(conn.__conn, __FILE__, __LINE__);

                    auto pollRes = conn.poll;
                    if(pollRes != CONNECTION_MADE)
                    {
                        if(waitForEstablishConn)
                        {
                            // waiting for any socket changes
                            auto set = new SocketSet;
                            set.add(conn.socket);

                            trace("waiting for any socket changes");
                            auto sockNum = Socket.select(set, null, set);
                            continue;
                        }
                        else
                        {
                            trace("connection isn't suitable for query, pollRes=", pollRes, ", conn status=", conn.status);
                            conn.destroy(); // reverts locked connection
                            continue pick_conn; // need try other connection
                        }
                    }

                    break;
                }

                return doQuery(conn);
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

private immutable(Result)[] doQuery(Connection conn)
{
    conn.sendQuery("SELECT 123, 567, 'asd fgh'");

    auto sock = conn.socket();

    import core.time;

    auto readSet = new SocketSet;
    auto errSet = new SocketSet;
    readSet.add(sock);
    errSet.add(sock);

    trace("waiting for data on the socket");
    auto sockNum = Socket.select(readSet, errSet, null, dur!"seconds"(10));

    if(sockNum == 0) // query timeout occured
    {
        trace("Exceeded Posgres query time limit");
        conn.cancel(); // cancel sql query
    }

    conn.consumeInput();

    immutable(Result)[] res;
    
    while(true)
    {
        auto r = conn.getResult();
        if(r is null) break;
        res ~= r;
    }

    if(sockNum == 0) // query timeout occured
        throw new PoolException("Exceeded Posgres query time limit", __FILE__, __LINE__);

    return res;
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

        auto results = pool.makeTransaction(args, true);

        import std.stdio;
        writeln("results=", results);

        foreach(r; results)
        {
            import std.stdio;
            writeln("res=", r.getAnswer);
        }

        results = pool.makeTransaction(args);

        foreach(r; results)
        {
            import std.stdio;
            writeln("res=", r.getAnswer);
        }
    }
}
