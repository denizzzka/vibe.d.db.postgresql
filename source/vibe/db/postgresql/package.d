/// PostgreSQL database client implementation.
module vibe.db.postgresql;

public import dpq2: ValueFormat;
public import dpq2.exception: Dpq2Exception;
public import dpq2.result;
public import dpq2.connection: ConnectionException, connStringCheck, ConnectionStart;
public import dpq2.args;
public import derelict.pq.pq;
import dpq2.async.waiter : SocketWaiter, SocketWaitMode;
public import dpq2.async.waiter : PostgresClientTimeoutException;
static import dpq2.async.connection;
static import dpq2.connection;

import vibe.core.core;
import vibe.core.connectionpool: ConnectionPool, VibeLockedConnection = LockedConnection;
import vibe.core.log;
import core.time;
import std.exception: assertThrown, enforce;
import std.conv: to;

///
struct ClientSettings
{
    string connString; /// PostgreSQL connection string
    void delegate(Connection) afterStartConnectOrReset; /// called after connection established
}

/// A Postgres client with connection pooling.
class PostgresClient
{
    private ConnectionPool!Connection pool;

    /// afterStartConnectOrReset is called after connection established
    this(
        string connString,
        uint connNum,
        void delegate(Connection) afterStartConnectOrReset = null
    )
    {
        immutable cs = ClientSettings(
            connString,
            afterStartConnectOrReset
        );

        this(&createConnection, cs, connNum);
    }

    /// Useful for external Connection implementation
    this
    (
        Connection delegate(in ClientSettings) @safe connFactory,
        immutable ClientSettings cs,
        uint connNum,
    )
    {
        cs.connString.connStringCheck;

        this(
            () @safe { return connFactory(cs); },
            connNum
        );
    }

    /// Useful for external Connection implementation
    ///
    /// Not cares about checking of connection string
    this(Connection delegate() @safe connFactory, uint connNum)
    {
        enforce(PQisthreadsafe() == 1);

        pool = new ConnectionPool!Connection(
                connFactory,
                connNum
            );
    }

    /// Get connection from the pool.
    ///
    /// Do not forgot to call .reset() for connection if ConnectionException
    /// was caught while using LockedConnection!
    LockedConnection lockConnection()
    {
        logDebugV("get connection from the pool");

        return pool.lockConnection();
    }

    /// Use connection from the pool.
    ///
    /// Same as lockConnection but automatically maintains initiation of
    /// reestablishing of connection by calling .reset()
    ///
    /// Returns: Value returned by delegate or void
    T pickConnection(T)(scope T delegate(scope LockedConnection conn) dg)
    {
        logDebugV("get connection from the pool");
        scope conn = pool.lockConnection();

        try
            return dg(conn);
        catch(ConnectionException e)
        {
            conn.reset(); // also may throw ConnectionException and this is normal behaviour

            throw e;
        }
    }

    ///
    private Connection createConnection(in ClientSettings cs) @safe
    {
        return new Connection(cs);
    }
}

private final class VibeSocketWaiter : SocketWaiter
{
    private FileDescriptorEvent ev;

    this(FileDescriptorEvent ev)
    {
        this.ev = ev;
    }

    override bool wait(SocketWaitMode mode, Duration timeout)
    {
        return ev.wait(timeout);
    }
}

///
alias LockedConnection = VibeLockedConnection!Connection;

/**
 * dpq2's AsyncConnection adopted for using with Vibe.d
 */
class Connection : dpq2.async.connection.AsyncConnection
{
    shared static immutable Duration pollingTimeout = dur!"seconds"(10); /// Timeout for use in polling loops etc
    Duration requestTimeout = dur!"seconds"(30); ///

    private const ClientSettings settings;
    ///
    this(const ref ClientSettings settings) @trusted
    {
        this.settings = settings;

        super(settings.connString, (int socket) => new VibeSocketWaiter(createReadSocketEvent(socket)),
            pollingTimeout, requestTimeout);

        logDebugV("creating new connection, delegate isNull="~(settings.afterStartConnectOrReset is null).to!string);

        if(settings.afterStartConnectOrReset !is null)
            settings.afterStartConnectOrReset(this);

        if(exec(`show client_encoding`).oneCell.as!string != "UTF8")
        {
            logWarn("client_encoding is not set to UTF8\nIt will be forced now to UTF8, but this behavior may be changed in the 2027."
                ~` Please add appropriate setting call exec("set client_encoding to 'UTF8'")`
                ~` into your connection factory or afterStartConnectOrReset delegate!`);

            exec("set client_encoding to 'UTF8'");
        }
    }

    /// Blocks while connection will be established or exception thrown
    override void reset()
    {
        super.reset();

        if(settings.afterStartConnectOrReset !is null)
            settings.afterStartConnectOrReset(this);
    }

    protected override void doQuery(void delegate() doesQueryAndCollectsResults)
    {
        logDebugV("doesQuery() call");
        super.doQuery(doesQueryAndCollectsResults);
    }

    protected override void runStatementBlockingMannerWithMultipleResults(void delegate() sendsStatementDg, void delegate(immutable(Result)) processResult, bool isRowByRowMode)
    {
        logDebugV(__FUNCTION__);

        try
        {
            super.runStatementBlockingMannerWithMultipleResults(sendsStatementDg, processResult, isRowByRowMode);
        }
        catch (PostgresClientTimeoutException e)
        {
            logDebugV("Exceeded Postgres query time limit");
            throw(e);
        }
    }
}

package auto createReadSocketEvent(T)(T socket)
{
    version(Posix)
    {
        import core.sys.posix.fcntl;
        import std.socket;
        assert((fcntl(cast(socket_t) socket, F_GETFL, 0) & O_NONBLOCK), "Socket assumed to be non-blocking already");
    }

    import socket_stuff : duplicateSocket;

    // vibe-core right now supports only read trigger event
    return createFileDescriptorEvent(cast(int) duplicateSocket(cast(int) socket), FileDescriptorEvent.Trigger.read);
}

unittest
{
    bool raised = false;

    try
    {
        auto client = new PostgresClient("wrong connect string", 2);
    }
    catch(ConnectionException e)
        raised = true;

    assert(raised);
}

version(AsyncIntegrationTests)
{
    import dpq2.async.tests : _integration_test;
    import vibe.core.core : runTask;
    import vibe.core.task : Task;

    private Task runDetachedTask(scope void delegate() dg)
    {
        auto d = cast(void delegate() nothrow) dg;
        return runTask(() nothrow @system { d(); });
    }

    void __integration_test(string connString)
    {
        _integration_test!(
            (int fd) => new VibeSocketWaiter(createReadSocketEvent(fd)),
            runDetachedTask,
            sleep
        )(connString);
    }
}
