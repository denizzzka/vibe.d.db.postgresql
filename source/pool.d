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
        trace("new connection is created");

        return c;
    }

    void makeTransaction(TransactionArgs args)
    {
        LockedConnection conn;

        // get usable connection and send SQL command
        while(true)
        {
            try
            {
                trace("get connection from pool");
                conn = lockConnection();
                trace("conn=", conn, ", send query");
                conn.sendQuery(args.sqlCommand);
            }
            catch(ConnectionException e)
            {
                warning("Connection failed");

                // try to restore connection because pool isn't do this job by itself
                try
                {
                    conn.connectStart();
                }
                catch(ConnectionException e){}

                conn.destroy(); // reverts locked connection
                continue;
            }

            break;
        }

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

version(IntegrationTest)
void __integration_test(string connString)
{
    auto pool = new ConnectionPool(connString, 2);

    {
        TransactionArgs args;
        args.sqlCommand = "SELECT 123";

        pool.makeTransaction(args);
    }
}
