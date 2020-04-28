#include <u.h>
#include <libc.h>
#include <thread.h>

#include "raftnet.h"

void
threadmain(int argc, char *argv[])
{
	RaftTCP *server[2];

	ARGBEGIN{
	}ARGEND

	server[0] = raft_tcp_new("tcp!192.168.7.127!12000");
	server[1] = raft_tcp_new("tcp!192.168.7.127!12001");

	raft_tcp_add_peer(server[0], 2, "tcp!192.168.7.127!12001");
	raft_tcp_add_peer(server[1], 1, "tcp!192.168.7.127!12000");

	for(;;)
		yield();

	threadexitsall(nil);
}
