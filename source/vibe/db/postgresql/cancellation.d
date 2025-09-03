///
module vibe.db.postgresql.cancellation;

import vibe.db.postgresql: Connection, createSocketEvent, PostgresClientTimeoutException;
import dpq2.cancellation;
import derelict.pq.pq;
import core.time: Duration;

///
package void cancelRequest(Connection conn, Duration timeout)
{
    auto c = new Cancellation(conn);
    c.start;

    while(true)
    {
        if(c.status == CONNECTION_BAD)
            throw new CancellationException(c.errorMessage);

        auto event = createSocketEvent(c.socketDuplicate);
        const r = c.poll;

        if(r == PGRES_POLLING_OK)
            break;
        else if(r == PGRES_POLLING_FAILED)
            throw new CancellationException(c.errorMessage);
        else
        {
            if(!event.wait(timeout))
                throw new PostgresClientTimeoutException("Exceeded Posgres query cancellation time limit", __FILE__, __LINE__);
        }

        continue;
    }
}
