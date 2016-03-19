module vibe.db.postgresql.pool;

import std.container.dlist;
import core.atomic: atomicOp;

synchronized class ConnectionPool(TConnection)
{
    private:

    TConnection delegate() connectionFactory;
    const uint maxConcurrent;
    uint lockedCount;
    DList!TConnection __freeConnections;

    public:

    this(TConnection delegate() @safe connectionFactory, uint maxConcurrent = uint.max)
    {
        this.connectionFactory = cast(shared) connectionFactory;
        this.maxConcurrent = maxConcurrent;
    }

    private DList!TConnection* freeConnections()
    {
        return cast(DList!TConnection*) &__freeConnections;
    }

    LockedConnection!TConnection lockConnection()
    {
        if(lockedCount < maxConcurrent)
        {
            lockedCount.atomicOp!"+="(1);

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
        else
        {
            assert(false); // FIXME
        }
    }

    private void releaseConnection(TConnection conn)
    {
        freeConnections.insertBack(conn);
        lockedCount.atomicOp!"-="(1);
    }
}

struct LockedConnection(TConnection)
{
    private shared ConnectionPool!TConnection pool;
    TConnection conn;
    alias conn this;

    private this(shared ConnectionPool!TConnection pool, TConnection conn)
    {
        this.pool = pool;
        this.conn = conn;
    }

    ~this()
    {
        pool.releaseConnection(conn);
    }

    @disable this(this){}
}
