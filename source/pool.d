module pgator2.pool;

import dpq2 = dpq2.connection;
import vibe = vibe.core.connectionpool;

class ConnectionPool : vibe.ConnectionPool!(dpq2.Connection)
{
    private const string connString;

    this(string connString, uint connNum)
    {
        this.connString = connString;

        super(&connectionFactory, connNum);
    }

    private dpq2.Connection connectionFactory()
    {
        auto c = new dpq2.Connection;
        c.connString = connString;
        c.connectStart;

        return c;
    }
}

unittest
{
    auto pool = new ConnectionPool("wrong connection string", 3);

    try
        pool.lockConnection;
    catch(dpq2.ConnectionException e)
        return;

    assert(false);
}
