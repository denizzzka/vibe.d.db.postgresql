module vibe.db.postgresql.windows;

version(Windows):

import core.time: Duration;
import core.sys.windows.winsock2;
import std.exception: enforce;
import std.conv: to;
import std.socket: SOCKET;
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
    //TODO: deregister events if struct destroyed?

    void wait()
    {
        if(!waitImpl(WSA_INFINITE))
            throw new ConnectionException("WSA_WAIT_TIMEOUT during WSA_INFINITE awaiting");
    }

    bool wait(Duration timeout)
    {
        return waitImpl(timeout.total!"msecs".to!int);
    }

    private bool waitImpl(DWORD timeout)
    {
        auto idx = WSAWaitForMultipleEvents(1, &event, false, timeout, false);

        if(idx == WSA_WAIT_FAILED)
            throw new ConnectionException("WSAWaitForMultipleEvents error, code "~WSAGetLastError().to!string);

        if(WSAResetEvent(event) == false)
            throw new ConnectionException("WSAResetEvent error, code "~WSAGetLastError().to!string);

        if(idx == WSA_WAIT_TIMEOUT)
            return false;

        return true;
    }
}

private:

alias WSAEVENT = void*;

// Not documented on MSDN
enum {
	FD_READ_BIT,
	FD_WRITE_BIT,
	FD_OOB_BIT,
	FD_ACCEPT_BIT,
	FD_CONNECT_BIT,
	FD_CLOSE_BIT,
	FD_QOS_BIT,
	FD_GROUP_QOS_BIT,
	FD_ROUTING_INTERFACE_CHANGE_BIT,
	FD_ADDRESS_LIST_CHANGE_BIT,
	FD_MAX_EVENTS // = 10
}

import core.sys.windows.winbase;

const WSA_INFINITE = INFINITE;
const WSA_INVALID_EVENT = WSAEVENT.init;
const WSA_WAIT_FAILED = cast(DWORD)-1;
const WSA_WAIT_TIMEOUT = WAIT_TIMEOUT;
const int FD_READ = 1 << FD_READ_BIT;

extern(Windows) nothrow @nogc
{
    import core.sys.windows.windef;

    WSAEVENT WSACreateEvent();
    int WSAEventSelect(SOCKET, WSAEVENT, int);
    BOOL WSAResetEvent(WSAEVENT);
    DWORD WSAWaitForMultipleEvents(DWORD, const(WSAEVENT)*, BOOL, DWORD, BOOL);
}
