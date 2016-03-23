module vibe.db.postgresql.pool;

import core.sync.semaphore;
import vibe.core.log;
import std.conv: to;
import core.atomic;

private synchronized class AtomicList(T)
{
    private:

    import std.container.dlist;
    import std.range;

    shared DList!T _storage;

    @property DList!T* storage(){ return cast(DList!T*) &_storage; }

    static struct Link
    {
        private shared typeof(_storage).Range _range;

        @property private auto range(){ return cast(typeof(_storage).Range*) &_range; }
        @property private ref auto elem(){ return range.front; }

        private this(typeof(_storage).Range r)
        {
            _range = cast(shared) r;

            debug
            {
                // size check
                size_t counter;

                foreach(e; *range)
                {
                    counter++;
                }

                assert(counter > 0);
            }
        }
    }

    Link store(T val)
    {
        storage.insertFront(val);
        Link ret = Link(storage.opSlice);

        return ret;
    }

    void remove(Link r)
    {
        storage.remove(*r.range);
    }

    Link getAndRemove(out bool success)
    {
        success = !storage.empty;

        Link ret;

        if(success)
        {
            ret = Link(storage.opSlice.dropBackOne);
        }

        return ret;
    }
}

shared class ConnectionPool(TConnection)
{
    private:

    AtomicList!TConnection connections;
    alias ConnLink = AtomicList!TConnection.Link;
    AtomicList!ConnLink freeConnections;

    Semaphore _maxConnSem;
    TConnection delegate() connectionFactory;

    @property ref auto maxConnSem(){ return cast() _maxConnSem; }

    public:

    this(TConnection delegate() connectionFactory, uint maxConcurrent = uint.max)
    {
        this.connectionFactory = cast(shared) connectionFactory;
        _maxConnSem = cast(shared) new Semaphore(maxConcurrent);
        connections = new shared AtomicList!TConnection;
        freeConnections = new shared AtomicList!ConnLink;
    }

    /// Non-blocking. Useful for fibers
    LockedConnection!TConnection tryLockConnection()
    {
        if(maxConnSem.tryWait)
        {
            logDebugV("lock connection");
            LockedConnection!TConnection conn = getConnection();
            return conn;
        }
        else
        {
            logDebugV("no free connections");
            return null;
        }
    }

    /// Blocking. Useful for threads
    @disable
    LockedConnection!TConnection lockConnection()
    {
        maxConnSem.wait();

        return getConnection();
    }

    private LockedConnection!TConnection getConnection()
    {
        scope(failure)
        {
            maxConnSem.notify();
            logDebugV("get connection aborted");
        }

        bool success;
        auto freeConnLink = freeConnections.getAndRemove(success);
        ConnLink link;

        if(success)
        {
            logDebugV("used connection return");
            link = freeConnLink.elem;
        }
        else
        {
            logDebugV("new connection return");
            TConnection conn = connectionFactory();
            assert(conn);
            link = connections.store(conn);
        }

        return new LockedConnection!TConnection(this, link);
    }

    /// If connection is null (means what connection was failed etc) it
    /// don't reverted to the connections list
    private void releaseConnection(ConnLink link)
    {
        logDebugV("release connection");
        if(link.elem !is null)
        {
            freeConnections.store(link);
        }
        else // failed state connection
        {
            connections.remove(link);
        }

        maxConnSem.notify();
    }
}

class LockedConnection(TConnection)
{
    private shared ConnectionPool!TConnection pool;
    private ConnectionPool!TConnection.ConnLink link;

    @property ref TConnection conn()
    {
        return link.elem;
    }

    package alias conn this;

    void dropConnection()
    {
        assert(conn);

        destroy(conn);
        conn = null;
    }

    private this(shared ConnectionPool!TConnection pool, ConnectionPool!TConnection.ConnLink link)
    {
        this.pool = pool;
        this.link = link;
    }

    ~this()
    {
        logDebugV("locked conn destructor");
        pool.releaseConnection(link);
    }
}
