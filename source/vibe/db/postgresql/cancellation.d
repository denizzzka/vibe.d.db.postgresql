///
module vibe.db.postgresql.cancellation;

import vibe.db.postgresql: Connection, createReadSocketEvent, PostgresClientTimeoutException;
import dpq2.cancellation;
import dpq2.socket_stuff: duplicateSocket;
import derelict.pq.pq;
import core.time: Duration;

///
package void cancelRequest(Connection conn, Duration timeout)
{
    auto c = new Cancellation(conn);
    c.start;
    auto event = createReadSocketEvent(c.socket.duplicateSocket);

    while(true)
    {
        if(c.status == CONNECTION_BAD)
            throw new CancellationException(c.errorMessage);

        const r = c.poll;

        if(r == PGRES_POLLING_OK)
            break;
        else if(r == PGRES_POLLING_FAILED)
            throw new CancellationException(c.errorMessage);
        else if(r == PGRES_POLLING_READING)
        {
            // On success cancellation socket will be closed without any
            // data receive and wait() will return false. So there is no
            // point in checking whether wait() was executed successfully
            event.wait(timeout);
        }

        continue;
    }
}

class CancellationTimeoutException : CancellationException
{
    this(string file = __FILE__, size_t line = __LINE__)
    {
        super("Exceeded cancellation time limit", file, line);
    }
}
