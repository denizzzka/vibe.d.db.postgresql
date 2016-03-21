module vibe.db.postgresql.pool;

import core.sync.semaphore;
import vibe.core.log;
import std.conv: to;
import core.atomic;

private synchronized class ConnectionsStorage(TConnection)
{
    private:

    import std.container.dlist;

    DList!TConnection freeConnections;
    size_t count = 0;

    TConnection getConnection()
    {
        atomicOp!"+="(count, 1);
        logDebugV("get conn, counter="~count.to!string);

        if((cast() freeConnections).empty)
        {
            return null;
        }
        else
        {
            TConnection conn = (cast() freeConnections).front;
            (cast() freeConnections).removeFront;
            return conn;
        }
    }

    void revertConnection(TConnection conn)
    {
        (cast() freeConnections).insertBack(conn);
        atomicOp!"-="(count, 1);
        logDebugV("revert conn, counter="~count.to!string);
    }
}

shared class ConnectionPool(TConnection)
{
    private:

    ConnectionsStorage!TConnection storage;
    TConnection delegate() connectionFactory;
    Semaphore maxConnSem;

    public:

    this(TConnection delegate() connectionFactory, uint maxConcurrent = uint.max)
    {
        this.connectionFactory = cast(shared) connectionFactory;
        storage = new shared ConnectionsStorage!TConnection;
        maxConnSem = cast(shared) new Semaphore(maxConcurrent);
    }

    LockedConnection!TConnection lockConnection()
    {
        logDebugV("try to lock connection, start wait");
        (cast() maxConnSem).wait();
        logDebugV("end wait");

        TConnection conn = storage.getConnection();

        if(conn !is null)
        {
            logDebugV("used connection return");
        }
        else
        {
            logDebugV("new connection return");
            conn = connectionFactory();
        }

        return LockedConnection!TConnection(this, conn);
    }

    private void releaseConnection(TConnection conn)
    {
        logDebugV("try to unlock connection");
        storage.revertConnection(conn);
        (cast() maxConnSem).notify();
        logDebugV("unlock done");
    }
}

struct LockedConnection(TConnection)
{
    private shared ConnectionPool!TConnection pool;
    private TConnection _conn;

    @property ref TConnection conn()
    {
        return _conn;
    }

    package alias conn this;

    private this(shared ConnectionPool!TConnection pool, TConnection conn)
    {
        this.pool = pool;
        this._conn = conn;
    }

    ~this()
    {
        pool.releaseConnection(conn);
    }

    @disable this(this){}
}
