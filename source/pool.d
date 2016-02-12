module pgator2.pool;

import dpq2.connection;
import std.exception;
import std.experimental.logger;
import std.container.dlist;
import core.time;

@property
private static DList!Connection unshare(shared(DList!Connection) s)
{
    return cast(DList!Connection) s;
}

/// Connections pool
synchronized class ConnectionPool
{
    private string connString;
    private size_t connNum;
    private TickDuration lastReanimationTry;

    private DList!Connection alive;
    private DList!Connection dead;


    this(string connString, size_t connNum)
    {
        this.connString = connString;
        this.connNum = connNum;

        foreach(i; 0..connNum)
        {
            auto c = new Connection;
            c.connString = connString;

            dead.unshare.insertBack(c);
        }

        lastReanimationTry = TickDuration.currSystemTick();
        reanimateConnections();
    }

    private void reanimateConnections()
    {
        if(!dead.unshare.empty)
        {
        }
    }

    private void reanimateConnection()
    {
        Connection conn = cast(Connection) dead.unshare.front;

        try
        {
            conn.connect;
        }
        catch(ConnectionException e)
        {
            warning("Postgres connection try is failed, reason: ", e.msg);

            dead.unshare.insertBack(dead.unshare.front);
            dead.unshare.removeFront(1);
        }
    }
}

class PoolException : Exception
{
    this(string msg, size_t file, size_t line)
    {
        super(msg, __FILE__, __LINE__);
    }
}
