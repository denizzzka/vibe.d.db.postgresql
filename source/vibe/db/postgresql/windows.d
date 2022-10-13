module vibe.db.postgresql.windows;

version(Windows):

import core.time: Duration;
import core.sys.windows.winsock2;
import std.exception: enforce;
import std.conv: to;
import dpq2.connection: ConnectionException;

package SocketEvent createFileDescriptorEvent(SOCKET socket, int trigger)
{
    WSAEVENT ev = WSACreateEvent();

    throwConnExHelper!WSACreateEvent(ev == WSA_INVALID_EVENT);
    throwConnExHelper!WSAEventSelect(WSAEventSelect(socket, ev, trigger));

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

        throwConnExHelper!WSAWaitForMultipleEvents(idx == WSA_WAIT_FAILED);
        throwConnExHelper!WSAResetEvent(WSAResetEvent(event) == false);

        if(idx == WSA_WAIT_TIMEOUT)
            return false;

        return true;
    }
}

package const int FD_READ = 1 << FD_READ_BIT;

private:

import std.traits;

auto throwConnExHelper(alias F)(int condition, string file = __FILE__, size_t line = __LINE__)
if(isCallable!F)
{
    enum useGetLastCode = true;

    if(condition)
        throw new ConnectionException(
            fullyQualifiedName!F~" error"~(useGetLastCode ? ", code "~WSAGetLastError().to!string : ""),
            file, line
        );
}

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

extern(Windows) nothrow @nogc
{
    import core.sys.windows.windef;

    WSAEVENT WSACreateEvent();
    int WSAEventSelect(SOCKET, WSAEVENT, int);
    BOOL WSAResetEvent(WSAEVENT);
    DWORD WSAWaitForMultipleEvents(DWORD, const(WSAEVENT)*, BOOL, DWORD, BOOL);
}
