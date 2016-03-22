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

    TConnection getConnection()
    {
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

    /// Non-blocking. Useful for fibers
    bool tryLockConnection(LockedConnection!TConnection* conn)
    {
        if((cast() maxConnSem).tryWait)
        {
            logDebugV("lock connection");
            *conn = getConnection();
            return true;
        }
        else
        {
            logDebugV("no free connections");
            return false;
        }
    }

    /// Blocking. Useful for threads
    LockedConnection!TConnection lockConnection()
    {
        (cast() maxConnSem).wait();

        return getConnection();
    }

    private LockedConnection!TConnection getConnection()
    {
        scope(failure) (cast() maxConnSem).notify();

        TConnection conn = storage.getConnection;

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

    /// If connection is null (means what connection was failed etc) it
    /// don't reverted to the connections list
    private void releaseConnection(TConnection conn)
    {
        if(conn !is null) storage.revertConnection(conn);

        (cast() maxConnSem).notify();
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

    void dropConnection()
    {
        assert(_conn);

        destroy(_conn);
        _conn = null;
    }

    private this(shared ConnectionPool!TConnection pool, TConnection conn)
    {
        this.pool = pool;
        this._conn = conn;
    }

    ~this()
    {
        if(pool) // TODO: remove this check
        {
            pool.releaseConnection(conn);
        }
    }

    @disable this(this){}
}
