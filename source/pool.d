module pgator2.pool;

import dpq2;
import std.exception;
import std.experimental.logger;
import std.container.dlist;
import core.time;
import std.parallelism;

@property
private static DList!Connection unshare(shared(DList!Connection) s)
{
    return cast(DList!Connection) s;
}

/// Connections pool
synchronized class ConnectionPool
{
    private DList!Connection alive;
    private DList!Connection dead;
    private DList!Connection inUse;
    private TickDuration lastReanimationTry;

    private TaskPool taskPool;

    this(string connString, size_t connNum)
    {
        foreach(i; 0..connNum)
        {
            auto c = new Connection;
            c.connString = connString;

            dead.unshare.insertBack(c);
        }

        taskPool = cast(shared) new TaskPool(4); // TODO: put postgresql connections num to here

        //lastReanimationTry = TickDuration.currSystemTick();
        //reanimateConnections();
    }

    private Connection getConnection()
    {
        if(alive.unshare.empty)
            throw new PoolException(PoolException.Type.NO_USABLE_CONNECTIONS, __FILE__, __LINE__);

        Connection c = alive.unshare.front;
        alive.unshare.removeFront(1);
        inUse.unshare.insertBack(c);

        return c;
    }

    private void retrieveConnection(Connection c, bool isAlive)
    {
        if(isAlive)
            alive.unshare.insertBack(c);
        else
            dead.unshare.insertBack(c);
    }

    private auto registerSessionTask(SessionDgArgs args)
    {
        auto t = sessionTask(this, args);
        (cast()taskPool).put(t);

        return t;
    }

    immutable(Result[]) makeTransaction(SessionDgArgs args)
    {
        auto t = registerSessionTask(args);
        t.yieldForce();

        // if time is out throw exception

        return null;
    }

    //~ private void reanimateConnections()
    //~ {
    // реанимировать надо до того как один не удастся реанимировать - тогда уже ждать
        //~ if(!dead.unshare.empty)
        //~ {
            //~ auto latest = dead.unshare.back;

            //~ while(!dead.unshare.empty)
            //~ {
                //~ reanimateConnection();
            //~ }
        //~ }

        //~ lastReanimationTry = TickDuration.currSystemTick();
    //~ }

    //~ private void reanimateConnection()
    //~ {
        //~ Connection conn = cast(Connection) dead.unshare.front;
        //~ dead.unshare.removeFront(1);

        //~ try
        //~ {
            //~ conn.connect;
        //~ }
        //~ catch(ConnectionException e)
        //~ {
            //~ warning("Postgres connection try is failed, reason: ", e.msg);

            //~ dead.unshare.insertBack(conn);

            //~ return;
        //~ }

        //~ alive.unshare.insertBack(conn);
    //~ }

    //~ {
        //~ Connection c = getConnection();

        //~ if(c is null)
        //~ {
            //~ Thread.sleep(dur!"seconds"(3));

            //~ c = getConnection();

            //~ if(c is null)
                //~ throw new PoolException("no free connections", __FILE__, __LINE__);
        //~ }

        //~ dg(c);
    //~ }
}

class PoolException : Exception
{
    enum Type
    {
        NO_USABLE_CONNECTIONS,
        ATTEMPTS_COUNT_EXPIRED
    }

    Type type;

    this(Type t, string file, size_t line)
    {
        type = t;

        super(to!string(type), file, line);
    }
}

struct SessionDgArgs
{
    Result[] delegate(Connection, SessionDgArgs) dg;
    size_t attemptsRemaining;
}

void sessionWorker(shared ConnectionPool pool, SessionDgArgs args)
{
    Connection conn = pool.getConnection;

    try
    {
        auto r = args.dg(conn, args);
    }
    catch(ConnectionException e)
    {
        warning("Connection error: ", e.msg, ", attempts remaining", args.attemptsRemaining);
        pool.retrieveConnection(conn, false);

        if(args.attemptsRemaining  > 0)
        {
            trace("Trying again");
            args.attemptsRemaining--;
            pool.registerSessionTask(args);
        }
        else
        {
            warning("No more attempts");
            throw new PoolException(PoolException.Type.ATTEMPTS_COUNT_EXPIRED, __FILE__, __LINE__);
        }
    }

    pool.retrieveConnection(conn, true);
}

alias sessionTask = task!(sessionWorker, shared ConnectionPool, SessionDgArgs);
