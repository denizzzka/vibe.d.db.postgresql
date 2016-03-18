module vibe.db.postgresql.pool;

@trusted:

import std.container.dlist;
import core.atomic: atomicOp;

shared class ConnectionPool(TConnection)
{
    private TConnection delegate() connectionFactory;
    private const uint maxConcurrent;
    private uint lockCount;
    private DList!TConnection __freeConnections;

    this(TConnection delegate() @safe connectionFactory, uint maxConcurrent = uint.max)
    {
        this.connectionFactory = cast(shared) connectionFactory;
        this.maxConcurrent = maxConcurrent;
    }

    private auto freeConnections()
    {
        return cast() __freeConnections;
    }

    synchronized LockedConnection!TConnection lockConnection()
    {
        if(lockCount < maxConcurrent)
        {
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

            atomicOp!"+="(lockCount, 1);
            return LockedConnection!TConnection(this, conn);
        }
        else
        {
            assert(false); // FIXME
        }
    }

    private synchronized void unlockConnection(LockedConnection!TConnection conn)
    {
        freeConnections.insertBack(conn.conn);
        atomicOp!"-="(lockCount, 1);
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
        pool.unlockConnection(this);
    }

    this(this){}
}
