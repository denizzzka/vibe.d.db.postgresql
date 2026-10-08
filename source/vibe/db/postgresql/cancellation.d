module vibe.db.postgresql.cancellation;

import vibe.db.postgresql : Connection;
import dpq2.async.cancellation;
import core.time : Duration;

deprecated("please use dpq2.async.cancellation.CancellationTimeoutException instead. CancellationTimeoutException will be removed after September 2027")
public alias CancellationTimeoutException = dpq2.async.cancellation.CancellationTimeoutException;

deprecated("please use Connection.cancelRequest() instead. cancelRequest() will be removed after September 2027")
package void cancelRequest(Connection conn, Duration timeout)
{
    conn.cancelRequest(timeout);
}