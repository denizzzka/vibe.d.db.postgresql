module pgator2.pool;

import dpq2 = dpq2.connection;
import vibe = vibe.core.connectionpool;

class ConnectionPool : vibe.ConnectionPool!(dpq2.Connection)
{
    this(string connString, uint connNum)
    {
        dpq2.Connection connectionFactory()
        {
            auto c = new dpq2.Connection;
            c.connString = connString;

            return c;
        }

        super(&connectionFactory, connNum);
    }
}
