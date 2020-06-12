/// PostgreSQL database client implementation.
module vibe.db.postgresql;

public import dpq2: ValueFormat;
public import dpq2.exception: Dpq2Exception;
public import dpq2.result;
public import dpq2.connection: ConnectionException, connStringCheck, ConnectionStart, CancellationException;
public import dpq2.args;
public import derelict.pq.pq;

import vibe.core.connectionpool: ConnectionPool, VibeLockedConnection = LockedConnection;
import vibe.core.log;
import core.time: Duration, dur;
import std.exception: enforce;
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

        pool = new ConnectionPool!Connection(() @safe { return connFactory(cs); }, connNum);
    }

    /// Useful for external Connection implementation
    ///
    /// Not cares about checking of connection string
    this(Connection delegate() const pure @safe connFactory, uint connNum)
    {
        enforce(PQisthreadsafe() == 1);

        pool = new ConnectionPool!Connection(
                () @safe { return connFactory(); },
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

alias Connection = Dpq2Connection;
deprecated("use Connection instead") alias __Conn = Connection;

///
alias LockedConnection = VibeLockedConnection!Connection;

/**
 * dpq2.Connection adopted for using with Vibe.d
 */
class Dpq2Connection : dpq2.Connection
{
    Duration socketTimeout = dur!"seconds"(10); ///
    Duration statementTimeout = dur!"seconds"(30); ///

    private const ClientSettings settings;

    ///
    this(const ref ClientSettings settings) @trusted
    {
        this.settings = settings;

        super(settings.connString);
        setClientEncoding("UTF8"); // TODO: do only if it is different from UTF8

        import std.conv: to;
        logDebugV("creating new connection, delegate isNull="~(settings.afterStartConnectOrReset is null).to!string);

        if(settings.afterStartConnectOrReset !is null)
            settings.afterStartConnectOrReset(this);
    }

    deprecated("please use .reset() instead")
    void resetStart()()
    {
        reset();
    }

    /// Blocks while connection will be established or exception thrown
    void reset()
    {
        super.resetStart;

        while(true)
        {
            if(status() == CONNECTION_BAD)
                throw new ConnectionException(this);

            if(resetPoll() != PGRES_POLLING_OK)
            {
                waitEndOfRead(socketTimeout);
                continue;
            }
            else
            {
                break;
            }
        }

        if(settings.afterStartConnectOrReset !is null)
            settings.afterStartConnectOrReset(this);
    }

    private auto waitEndOfRead(in Duration timeout) // TODO: rename to waitEndOf + add FileDescriptorEvent.Trigger argument
    {
        import vibe.core.core;

        version(Posix)
        {
            import core.sys.posix.fcntl;
            assert((fcntl(this.posixSocket, F_GETFL, 0) & O_NONBLOCK), "Socket assumed to be non-blocking already");
        }

        version(Have_vibe_core)
        {
            // vibe-core right now supports only read trigger event
            // it also closes the socket on scope exit, thus a socket duplication here
            auto event = createFileDescriptorEvent(this.posixSocketDuplicate, FileDescriptorEvent.Trigger.read);
        }
        else
        {
            auto event = createFileDescriptorEvent(this.posixSocket, FileDescriptorEvent.Trigger.any);
        }

        return event;
    }

    private void waitEndOfReadAndConsume(in Duration timeout)
    {
        auto event = waitEndOfRead(timeout);
        scope(exit) destroy(event); // Prevents 100% CPU usage

        do
        {
            if(!event.wait(timeout))
                throw new PostgresClientTimeoutException(__FILE__, __LINE__);

            consumeInput();
        }
        while (this.isBusy); // wait until PQgetresult won't block anymore
    }

    private void doQuery(void delegate() doesQueryAndCollectsResults)
    {
        // Try to get usable connection and send SQL command
        while(true)
        {
            if(status() == CONNECTION_BAD)
                throw new ConnectionException(this, __FILE__, __LINE__);

            if(poll() != PGRES_POLLING_OK)
            {
                waitEndOfReadAndConsume(socketTimeout);
                continue;
            }
            else
            {
                break;
            }
        }

        logDebugV("doesQuery() call");
        doesQueryAndCollectsResults();
    }

    private immutable(Result) runStatementBlockingManner(void delegate() sendsStatementDg)
    {
        immutable(Result)[] res;

        runStatementBlockingMannerWithMultipleResults(sendsStatementDg, (r){ res ~= r; }, false);

        enforce(res.length == 1, "Simple query without row-by-row mode can return only one Result instance, not "~res.length.to!string);

        return res[0];
    }

    private void runStatementBlockingMannerWithMultipleResults(void delegate() sendsStatementDg, void delegate(immutable(Result)) processResult, bool isRowByRowMode)
    {
        logDebugV(__FUNCTION__);
        immutable(Result)[] res;

        doQuery(()
            {
                sendsStatementDg();

                if(isRowByRowMode)
                {
                    enforce(setSingleRowMode, "Failed to set row-by-row mode");
                }

                scope(failure)
                {
                    if(isRowByRowMode)
                        while(getResult() !is null){} // autoclean of results queue
                }

                scope(exit)
                {
                    logDebugV("consumeInput()");
                    consumeInput(); // TODO: redundant call (also called in waitEndOfRead) - can be moved into catch block?

                    while(true)
                    {
                        logDebugV("getResult()");
                        auto r = getResult();

                        /*
                         I am trying to check connection status with PostgreSQL server
                         with PQstatus and it always always return CONNECTION_OK even
                         when the cable to the server is unplugged.
                                                    – user1972556 (stackoverflow.com)

                         ...the idea of testing connections is fairly silly, since the
                         connection might die between when you test it and when you run
                         your "real" query. Don't test connections, just use them, and
                         if they fail be prepared to retry everything since you opened
                         the transaction. – Craig Ringer Jan 14 '13 at 2:59
                         */
                        if(status == CONNECTION_BAD)
                            throw new ConnectionException(this, __FILE__, __LINE__);

                        if(r is null) break;

                        processResult(r);
                    }
                }

                try
                {
                    waitEndOfReadAndConsume(statementTimeout);
                }
                catch(PostgresClientTimeoutException e)
                {
                    logDebugV("Exceeded Posgres query time limit");

                    try
                        cancel(); // cancel sql query
                    catch(CancellationException ce) // means successful cancellation
                        e.msg ~= ", "~ce.msg;

                    throw e;
                }
            }
        );
    }

    ///
    immutable(Answer) execStatement(
        string sqlCommand,
        ValueFormat resultFormat = ValueFormat.BINARY
    )
    {
        QueryParams p;
        p.resultFormat = resultFormat;
        p.sqlCommand = sqlCommand;

        return execStatement(p);
    }

    ///
    immutable(Answer) execStatement(in ref QueryParams params)
    {
        auto res = runStatementBlockingManner({ sendQueryParams(params); });

        return res.getAnswer;
    }

    /// Row-by-row version of execStatement
    ///
    /// Delegate called for each received row.
    ///
    /// More info: https://www.postgresql.org/docs/current/libpq-single-row-mode.html
    ///
    void execStatementRbR(
        string sqlCommand,
        void delegate(immutable(Row)) answerRowProcessDg,
        ValueFormat resultFormat = ValueFormat.BINARY
    )
    {
        QueryParams p;
        p.resultFormat = resultFormat;
        p.sqlCommand = sqlCommand;

        execStatementRbR(p, answerRowProcessDg);
    }

    /// Ditto
    void execStatementRbR(in ref QueryParams params, void delegate(immutable(Row)) answerRowProcessDg)
    {
        runStatementWithRowByRowResult(
            { sendQueryParams(params); },
            answerRowProcessDg
        );
    }

    private void runStatementWithRowByRowResult(void delegate() sendsStatementDg, void delegate(immutable(Row)) answerRowProcessDg)
    {
        runStatementBlockingMannerWithMultipleResults(
                sendsStatementDg,
                (r)
                {
                    auto answer = r.getAnswer;

                    enforce(answer.length <= 1, `0 or 1 rows can be received, not `~answer.length.to!string);

                    if(answer.length == 1)
                    {
                        enforce(r.status == PGRES_SINGLE_TUPLE, `Wrong result status: `~r.status.to!string);

                        answerRowProcessDg(answer[0]);
                    }
                },
                true
            );
    }

    ///
    void prepareStatement(
        string statementName,
        string sqlStatement,
        Oid[] oids = null
    )
    {
        auto r = runStatementBlockingManner(
                {sendPrepare(statementName, sqlStatement, oids);}
            );

        if(r.status != PGRES_COMMAND_OK)
            throw new ResponseException(r, __FILE__, __LINE__);
    }

    ///
    immutable(Answer) execPreparedStatement(in ref QueryParams params)
    {
        auto res = runStatementBlockingManner({ sendQueryPrepared(params); });

        return res.getAnswer;
    }

    ///
    immutable(Answer) describePreparedStatement(string preparedStatementName)
    {
        auto res = runStatementBlockingManner({ sendDescribePrepared(preparedStatementName); });

        return res.getAnswer;
    }

    /**
     * Non blocking method to wait for next notification.
     *
     * Params:
     *      timeout = maximal duration to wait for the new Notify to be received
     *
     * Returns: New Notify or null when no other notification is available or timeout occurs.
     * Throws: ConnectionException on connection failure
     */
    Notify waitForNotify(in Duration timeout = Duration.max)
    {
        // try read available
        auto ntf = getNextNotify();
        if (ntf !is null) return ntf;

        // wait for next one
        try waitEndOfReadAndConsume(timeout);
        catch (PostgresClientTimeoutException) return null;
        return getNextNotify();
    }
}

///
class PostgresClientTimeoutException : Dpq2Exception
{
    this(string file, size_t line)
    {
        super("Exceeded Posgres query time limit", file, line);
    }
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

version(IntegrationTest) void __integration_test(string connString)
{
    setLogLevel = LogLevel.debugV;

    auto client = new PostgresClient(connString, 3);

    auto conn = client.lockConnection;
    {
        auto res = conn.execStatement(
            "SELECT 123::integer, 567::integer, 'asd fgh'::text",
            ValueFormat.BINARY
        );

        assert(res.getAnswer[0][1].as!PGinteger == 567);
    }

    {
        // Row-by-row result receiving
        int[] res;

        conn.execStatementRbR(
            `SELECT generate_series(0, 3) as i, pg_sleep(0.2)`,
            (immutable(Row) r)
            {
                res ~= r[0].as!int;
            }
        );

        assert(res.length == 4);
    }

    {
        // Row-by-row result receiving: error while receiving
        size_t rowCounter;

        QueryParams p;
        p.sqlCommand =
            `SELECT 1.0 / (generate_series(1, 100000) % 80000)`; // division by zero error at generate_series=80000

        bool assertThrown;

        try
            conn.execStatementRbR(p,
                (immutable(Row) r)
                {
                    rowCounter++;
                }
            );
        catch(ResponseException) // catches ERROR:  division by zero
            assertThrown = true;

        assert(assertThrown);
        assert(rowCounter > 0);
    }

    {
        QueryParams p;
        p.sqlCommand = `SELECT 123`;

        auto res = conn.execStatement(p);

        assert(res.length == 1);
        assert(res[0][0].as!int == 123);
    }

    {
        conn.prepareStatement("stmnt_name", "SELECT 123::integer");

        bool throwFlag = false;

        try
            conn.prepareStatement("wrong_stmnt", "WRONG SQL STATEMENT");
        catch(ResponseException)
            throwFlag = true;

        assert(throwFlag);
    }

    {
        import dpq2.oids: OidType;

        auto a = conn.describePreparedStatement("stmnt_name");

        assert(a.nParams == 0);
        assert(a.OID(0) == OidType.Int4);
    }

    {
        QueryParams p;
        p.preparedStatementName = "stmnt_name";

        auto r = conn.execPreparedStatement(p);

        assert(r.getAnswer[0][0].as!PGinteger == 123);
    }

    {
        // Fibers test
        import vibe.core.concurrency;

        auto future0 = async({
            client.pickConnection(
                (scope c)
                {
                    immutable answer = c.execStatement("SELECT 'New connection 0'");
                }
            );

            return 1;
        });

        auto future1 = async({
            client.pickConnection(
                (scope c)
                {
                    immutable answer = c.execStatement("SELECT 'New connection 1'");
                }
            );

            return 1;
        });

        immutable answer = conn.execStatement("SELECT 'Old connection'");

        assert(future0 == 1);
        assert(future1 == 1);
        assert(answer.length == 1);
    }

    {
        assert(conn.escapeIdentifier("abc") == "\"abc\"");
    }

    {
        import core.time : msecs;
        import vibe.core.core : sleep;
        import vibe.core.concurrency : async;

        struct NTF {string name; string extra;}

        auto futureNtf = async({
            Notify pgNtf;

            client.pickConnection(
                (scope c)
                {
                    c.execStatement("LISTEN foo");
                    pgNtf = c.waitForNotify();
                }
            );

            assert(pgNtf !is null);
            return NTF(pgNtf.name.idup, pgNtf.extra.idup);
        });

        sleep(10.msecs);
        conn.execStatement("NOTIFY foo, 'bar'");

        assert(futureNtf.name == "foo");
        assert(futureNtf.extra == "bar");
    }
}
