module pgator2.pool;

import dpq2;
import vibe = vibe.core.connectionpool;
import std.experimental.logger;

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

    immutable(Result)[] makeTransaction(TransactionArgs args)
    {
        LockedConnection conn;

        // get usable connection and send SQL command
        conn_loop:
        foreach(unused_var; 0..100)
        {
            try
            {
                trace("get connection from pool");
                conn = lockConnection();

                // obtained connection should be ready to send

                if(conn.status == CONNECTION_BAD) // need to reconnect this connection
                    throw new ConnectionException(conn.__conn, __FILE__, __LINE__);

                auto pollRes = conn.poll;
                if(pollRes != CONNECTION_MADE)
                {
                    trace("connection isn't suitable to do query, pollRes=", pollRes, ", conn status=", conn.status);
                    conn.destroy(); // reverts locked connection
                    continue; // need try other connection
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

            assert(false);
        }

        return null;
    }
}

private immutable(Result)[] doQuery(Connection conn)
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
    auto sockNum = Socket.select(readSet, errSet, null, dur!"seconds"(10));

    if(sockNum == 0) // query timeout occured
    {
        warning("Exceeded Posgres query time limit");
        conn.cancel(); // cancel query
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

        auto results = pool.makeTransaction(args);

        foreach(r; results)
        {
            import std.stdio;
            writeln("res=", r.getAnswer);
        }
    }
}
