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
        foreach(unused_var; 0..6)
        {
            try
            {
                trace("get connection from pool");
                conn = lockConnection();

                // obtained connection should be ready to send

                if(conn.status == CONNECTION_BAD) // need to reconnect this connection
                    throw new ConnectionException(conn.__conn, __FILE__, __LINE__);

                while(true) // data transfer
                {
                    auto sock = conn.socket();

                    import std.socket;
                    auto readWriteSet = new SocketSet;
                    auto errSet = new SocketSet;

                    switch(conn.poll)
                    {
                        case PGRES_POLLING_READING:
                            trace("waiting for socket ready for reading");
                            sock.select(readWriteSet, null, errSet); // waiting for socket ready for reading
                            continue; // need polling again

                        case PGRES_POLLING_WRITING:
                            trace("waiting for socket ready for writing");
                            sock.select(null, readWriteSet, errSet); // waiting for socket ready for writing
                            continue; // need polling again

                        case PGRES_POLLING_OK:
                            trace("sending query");
                            conn.sendQuery(args.sqlCommand);

                            conn.consumeInput();

                            immutable(Result)[] res;

                            while(true)
                            {
                                auto r = conn.getResult();
                                if(r)
                                    res ~= r;
                                else
                                    break;
                            }

                            //revert connection
                            conn.destroy(); // reverts locked connection to pool
                            return res;

                        case PGRES_POLLING_FAILED:
                            throw new ConnectionException(conn.__conn, __FILE__, __LINE__);

                        default:
                            continue;
                    }
                }

                // unsuitable to send state of connection, revert it to pool and try another

                trace("unsuitable to send, status=", conn.status);
                conn.destroy(); // reverts locked connection to pool
                continue;
            }
            catch(ConnectionException e)
            {
                // this block just starts reconnection and immediately loops back
                warning("Connection failed");

                // try to restore connection because pool isn't do this job by itself
                try
                {
                    conn.disconnect();
                    conn.connectStart();
                }
                catch(ConnectionException e){}

                conn.destroy(); // reverts locked connection
                continue;
            }

            assert(false);
        }

        assert(false);
        // awaiting for answer
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
            import std.stdio; writeln(r);
        }
    }
}
