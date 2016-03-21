module vibe.db.postgresql.pool;

import std.container.dlist;
import core.sync.semaphore;

shared class ConnectionPool(TConnection)
{
    private:

    TConnection delegate() connectionFactory;
    __gshared Semaphore maxConnSem;
    uint lockedCount;
    DList!TConnection __freeConnections;

    DList!TConnection* freeConnections()
    {
        return cast(DList!TConnection*) &__freeConnections;
    }

    public:

    this(TConnection delegate() connectionFactory, uint maxConcurrent = uint.max)
    {
        this.connectionFactory = cast(shared) connectionFactory;
        maxConnSem = new Semaphore(maxConcurrent);
    }

    LockedConnection!TConnection lockConnection()
    {
        maxConnSem.wait();

        TConnection conn;

        if(freeConnections.empty)
        {
            conn = connectionFactory();
        }
        else
        {
            conn = freeConnections.front;
            freeConnections.removeFront;
        }

        return LockedConnection!TConnection(this, conn);
    }

    private void releaseConnection(TConnection conn)
    {
        freeConnections.insertBack(conn);
        maxConnSem.notify();
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
