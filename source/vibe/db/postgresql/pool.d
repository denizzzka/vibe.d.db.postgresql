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

    this()
    {
        _storage = cast(shared) new DList!T;
    }

    Take!(DList!T.Range) store(T val)
    {
        storage.insertFront(val);

        return storage.opSlice.take(1);
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

    AtomicList!TConnection _connections;
    alias ConnLink = AtomicList!TConnection.Link;
    //AtomicList!ConnLink _freeConnections;

    //~ Semaphore _maxConnSem;
    //~ TConnection delegate() connectionFactory;

    //~ @property ref auto connections(){ return cast() _connections; }
    //~ @property ref auto freeConnections(){ return cast() _freeConnections; }
    //~ @property ref auto maxConnSem(){ return cast() _maxConnSem; }

    //~ public:

    //~ this(TConnection delegate() connectionFactory, uint maxConcurrent = uint.max)
    //~ {
        //~ this.connectionFactory = cast(shared) connectionFactory;
        //~ _connections = new shared AtomicList!TConnection;
        //~ _freeConnections = new shared AtomicList!ConnLink;
        //~ _maxConnSem = cast(shared) new Semaphore(maxConcurrent);
    //~ }

    //~ /// Non-blocking. Useful for fibers
    //~ bool tryLockConnection(LockedConnection!TConnection* conn)
    //~ {
        //~ if(maxConnSem.tryWait)
        //~ {
            //~ logDebugV("lock connection");
            //~ *conn = getConnection();
            //~ return true;
        //~ }
        //~ else
        //~ {
            //~ logDebugV("no free connections");
            //~ return false;
        //~ }
    //~ }

    //~ /// Blocking. Useful for threads
    //~ LockedConnection!TConnection lockConnection()
    //~ {
        //~ maxConnSem.wait();

        //~ return getConnection();
    //~ }

    //~ private LockedConnection!TConnection getConnection()
    //~ {
        //~ scope(failure)
        //~ {
            //~ maxConnSem.notify();
            //~ logDebugV("get connection aborted");
        //~ }

        //~ bool getResult;
        //~ AtomicList!ConnLink conn = freeConnections.getAndRemove(getResult);

        //~ if(getResult)
        //~ {
            //~ logDebugV("used connection return");
        //~ }
        //~ else
        //~ {
            //~ logDebugV("new connection return");
            //~ conn = connections.store(connectionFactory());
        //~ }

        //~ return new LockedConnection!TConnection(this, conn);
    //~ }

    //~ /// If connection is null (means what connection was failed etc) it
    //~ /// don't reverted to the connections list
    //~ private void releaseConnection(TConnection conn)
    //~ {
        //~ logDebugV("release connection");
        //~ if(conn !is null)
        //~ {
            //~ freeConnections.store(conn);
        //~ }
        //~ else // failed state connection
        //~ {
            //~ connections.remove(conn);
        //~ }

        //~ maxConnSem.notify();
    //~ }
}

class LockedConnection(TConnection)
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
        logDebugV("locked conn destructor");

        if(pool) // TODO: remove this check
        {
            pool.releaseConnection(conn);
        }
    }

    //@disable this(this){}
}
