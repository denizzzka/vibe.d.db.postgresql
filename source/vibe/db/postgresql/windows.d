module vibe.db.postgresql.windows;

version(Windows):

import core.time: Duration, dur;
import std.exception: enforce;
import std.conv: to;
import std.socket: SOCKET;
import windows.winsock2;
import dpq2.connection: ConnectionException;

package SocketEvent createFileDescriptorEvent(SOCKET _socket)
{
    //FIXME: SOCKET sizeof mismatch. need to remove cast. Details: https://github.com/etcimon/windows-headers/issues/12
    auto socket = cast(int) _socket;

    WSAEVENT ev = WSACreateEvent();

    if(ev == WSA_INVALID_EVENT)
        throw new ConnectionException("WSACreateEvent error, code "~WSAGetLastError().to!string);

    auto r = WSAEventSelect(socket, ev, FD_READ);

    if(r)
        throw new ConnectionException("WSAEventSelect error, code "~WSAGetLastError().to!string);

    return SocketEvent(ev);
}

struct SocketEvent
{
    private WSAEVENT event;

    void wait()
    {
        assert(false);
    }

    bool wait(Duration timeout)
    {
        assert(false);
    }
}
