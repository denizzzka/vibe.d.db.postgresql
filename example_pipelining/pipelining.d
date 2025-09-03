module vibe.db.postgresql.pipelining;

import std.getopt;
import std.exception;
import vibe.d;
import vibe.db.postgresql;

PostgresClient client;

void main(string[] args)
{
    enforce(PQlibVersion() >= 14_0000);

    string connString;
    getopt(args, "conninfo", &connString);

    void initConnectionDg(Connection conn)
    {
        conn.exec(`set client_encoding to 'UTF8'`);
        conn.exec(`set statement_timeout to '15 s'`);
    }

    client = new PostgresClient(connString, 1, &initConnectionDg);

    foreach(_; 0 .. 10)
    {
        assertThrown(
            client.pickConnection!void((conn) => requestsInPipeline(conn, true))
        );

        client.pickConnection!void((conn) => requestsInPipeline(conn, false));
    }

    logInfo("Done!");
}

void requestsInPipeline(scope LockedConnection conn, in bool shouldFail)
{
    assert(conn.pipelineStatus == PGpipelineStatus.PQ_PIPELINE_OFF, conn.pipelineStatus.to!string);
    assert(conn.transactionStatus == PQTRANS_IDLE, conn.transactionStatus.to!string);

    conn.enterPipelineMode;
    scope(exit)
    {
        // Remove remaining results
        while(conn.pipelineStatus == PGpipelineStatus.PQ_PIPELINE_ABORTED)
            conn.getResult(conn.pollingTimeout);

        conn.exitPipelineMode;

        // Finish possible opened transaction
        if(conn.transactionStatus != PQTRANS_IDLE)
            conn.exec("ROLLBACK"); // not pipelined
    }

    conn.sendQuery("BEGIN READ ONLY");

    conn.sendQuery("select id, 'test string' as txt from generate_series(1, 500) AS id");

    {
        QueryParams p;
        p.sqlCommand =
            "SELECT 123 as a, 456 as b, 789 as c, $1 as d "~
            "UNION ALL "~
            "SELECT 0, 1, 2, $2" ~ (shouldFail ? "'wrong field'" : "");
        p.argsVariadic(-5, -7);
        conn.sendQueryParams(p);
    }

    // Record command for server to flush its own buffer into client
    conn.sendFlushRequest;

    // Client buffer flushing, server receives it and starts processing
    conn.flush();

    {
        auto p = QueryParams(sqlCommand: "SELECT $1 as single");
        p.argsVariadic(31337);
        conn.sendQueryParams(p);
    }

    conn.sendQuery("COMMIT");

    conn.pipelineSync;

    auto processNextResult(in ConnStatusType expectedStatus)
    {
        auto r = conn.getResult(conn.requestTimeout);
        enforce(r.status == expectedStatus, "status="~r.statusString);

        if(expectedStatus != PGRES_PIPELINE_SYNC)
        {
            // Read result delimiter
            enforce(conn.getResult(conn.requestTimeout) is null);
        }

        return r;
    }

    auto processNextAnswerRowByRow()
    {
        auto r = conn.getResult(conn.requestTimeout);

        enforce(r !is null, "Unexpected delimiter");

        if(r.status == PGRES_TUPLES_OK)
        {
            assert(r.getAnswer.length == 0, "End of table expected");

            enforce(conn.getResult(conn.requestTimeout) is null, "Result delimiter expected");

            return null;
        }

        enforce(r.status == PGRES_SINGLE_TUPLE, "status="~r.statusString);

        return r.getAnswer;
    }

    processNextResult(PGRES_COMMAND_OK); // BEGIN

    // Single row read
    {
        conn.setSingleRowModeEx();

        while(auto r = processNextAnswerRowByRow())
        {
            auto row = r.oneRow;
            auto id = row["id"].as!int;
            assert(row["txt"].as!string == "test string");
        }
    }

    auto r2 = processNextResult(PGRES_TUPLES_OK).getAnswer;
    auto r3 = processNextResult(PGRES_TUPLES_OK).getAnswer;
    processNextResult(PGRES_COMMAND_OK); // COMMIT
    processNextResult(PGRES_PIPELINE_SYNC); // End of pipeline

    assert(r2[1]["d"].as!int == -7);
    assert(r3.oneCell.as!int == 31337);
}
