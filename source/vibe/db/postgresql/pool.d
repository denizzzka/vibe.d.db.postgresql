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

    DList!T _storage;
    alias Link = DList!T.Range;

    @property ref auto storage(){ return cast() _storage; }

    DList!T.Range store(T val)
    {
        storage.insertFront(val);

        return storage.opSlice;
    }

    void remove(Link r)
    {
        storage.remove(r);
    }

    Link getAndRemove(out bool success)
    {
        success = !storage.empty;

        Link ret;

        if(success)
        {
            ret = storage.opSlice.dropBackOne;
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

    //~ /// Blocking. Useful for threads
    //~ LockedConnection!TConnection lockConnection()
    //~ {
        //~ maxConnSem.wait();

        //~ return getConnection();
    //~ }

    private LockedConnection!TConnection getConnection()
    {
        scope(failure)
        {
            maxConnSem.notify();
            logDebugV("get connection aborted");
        }

        bool success;
        ConnLink conn = freeConnections.getAndRemove(success).front;

        if(success)
        {
            logDebugV("used connection return");
        }
        else
        {
            logDebugV("new connection return");
            conn = connections.store(connectionFactory());
        }

        return new LockedConnection!TConnection(conn);
    }

    /// If connection is null (means what connection was failed etc) it
    /// don't reverted to the connections list
    private void releaseConnection(ConnLink link)
    {
        logDebugV("release connection");
        if(link.front !is null)
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
        return (cast() link).front;
    }

    package alias conn this;

    void dropConnection()
    {
        assert(conn);

        destroy(conn);
        conn = null;
    }

    private this(ConnectionPool!TConnection.ConnLink link)
    {
        this.link = link;
    }

    ~this()
    {
        logDebugV("locked conn destructor");
        pool.releaseConnection(link);
    }
}
