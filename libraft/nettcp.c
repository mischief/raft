#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include <cbor.h>

#include "consensus.h"
#include "impl.h"
#include "raftnet.h"
#include "serialize.h"

enum {
	CMD_APPEND_ENTRIES = 0,
	CMD_REQUEST_VOTE,
};

static long
_iowerrstr(va_list *arg)
{
	char *str;

	str = va_arg(*arg, char*);
	werrstr(str);
	return 0;
}

long
iowerrstr(Ioproc *io, char *str)
{
	return iocall(io, _iowerrstr, str);
}

void
netlog(char *fmt, ...)
{
	va_list arg;
	char *new;

	// ok, concurrency.
	new = smprint("%s\n", fmt);
	if(new == nil)
		sysfatal("oom: %r");

	va_start(arg, fmt);
	vfprint(2, new, arg);
	va_end(arg);
	free(new);
}

struct RaftTCP
{
	QLock;

	Consensus *r;

	int afd;
	char addr[128];
	char adir[NETPATHLEN];

	Intmap *outgoing;

	int listenerpid;
};

static void
_listener(void*);

RaftTCP*
raft_tcp_new(char *addr)
{
	RaftTCP *rt;

	rt = raftmalloc(sizeof(*rt));

	snprint(rt->addr, sizeof(rt->addr), "%s", addr);
	rt->afd = announce(addr, rt->adir);
	if(rt->afd < 0)
		sysfatal("announce: %r");

	rt->outgoing = allocmap(nil);

	rt->listenerpid = proccreate(_listener, rt, 8192);

	return rt;
}

void
raft_tcp_set_server(RaftTCP *rt, Consensus *c)
{
	qlock(rt);
	rt->r = c;
	qunlock(rt);
}

typedef struct RaftTCPPeer RaftTCPPeer;
struct RaftTCPPeer
{
	QLock;
	Rendez r;
	s64int peer;
	int fd;
	char addr[128];
	int thrid;
	Ioproc *io;
};

/* thread to (re)establish outgoing connections */
static void
_outgoing(void *v)
{
	RaftTCPPeer *rtp;
	int fd;

	rtp = v;

	for(;;){
		while(rtp->fd >= 0)
			rsleep(&rtp->r);

		qunlock(rtp);	

		fd = -1;
		while(fd < 0){
			netlog("dialing peer %lld@%s...", rtp->peer, rtp->addr);
			fd = iodial(rtp->io, rtp->addr, nil, nil, nil);
			if(fd < 0){
				netlog("dialing %lld@%s failed: %r", rtp->peer, rtp->addr);
				iosleep(rtp->io, 1000+nrand(3000));
			}
		}

		netlog("connection established to %lld@%s", rtp->peer, rtp->addr);

		qlock(rtp);
		rtp->fd = fd;
	}
}

void
raft_tcp_add_peer(RaftTCP *rt, s64int peer, char *addr)
{
	RaftTCPPeer *rtp;

	rtp = raftmalloc(sizeof(*rtp));
	rtp->r.l = rtp;
	rtp->peer = peer;
	rtp->fd = -1;
	snprint(rtp->addr, sizeof(rtp->addr), "%s", addr);

	rtp->io = ioproc();

	qlock(rtp);

	rtp->thrid = threadcreate(_outgoing, rtp, 8192);

	insertkey(rt->outgoing, (ulong) peer, rtp);
}

static int
reply_and_free(int fd, uchar *buf, ulong sz)
{
	int rv;
	uchar hdr[4];

	rv = -1;

	PBIT32(hdr, sz);

	if(write(fd, hdr, 4) != 4)
		goto out;

	if(write(fd, buf, sz) != sz)
		goto out;

	rv = 0;

out:
	free(buf);
	return rv;
}

/* process to dispatch RPCs arriving from remote peers */
static void
_incoming(void *v)
{
	void **vv;
	RaftTCP *rt;
	int fd, rv;
	NetConnInfo *nci;
	char raddr[128];
	uchar buf[4+2]; // size + cmd
	uchar *data, *outdata;
	u32int szin;
	ulong szout;
	u16int cmd;

	netlog("incoming!");

	vv = v;
	rt = vv[0];
	fd = (uintptr) vv[1];

	free(vv);

	nci = getnetconninfo(nil, fd);
	snprint(raddr, sizeof(raddr), "%s", nci->raddr);
	freenetconninfo(nci);

	netlog("call from: %s", raddr);

	threadsetname("call from %s", raddr);

	for(;;){
		werrstr("short read");
		if(readn(fd, buf, sizeof(buf)) != sizeof(buf)){
			netlog("read size %s: %r", raddr);
			break;
		}

		szin = GBIT32(buf);
		cmd = GBIT16(buf+4);

		data = raftmalloc(szin);

		werrstr("short read");
		if(readn(fd, data, szin) != szin){
			netlog("read data %s: %r", raddr);
			break;
		}

		switch(cmd){
		case CMD_APPEND_ENTRIES:
			;
			AppendEntriesRequest aer;
			AppendEntriesReply r;

			rv = appendentries_request_decode(&aer, data, szin);
			free(data);
			if(rv < 0){
				netlog("appendentries_request_decode failed: %r");
				goto closeit;
			}

			qlock(rt);
			if(rt->r == nil){
				qunlock(rt);
				netlog("no raft to hand");
				appendentries_free(&aer);
				goto closeit;
			}

			consensus_append(rt->r, &aer, &r);

			qunlock(rt);

			appendentries_free(&aer);

			if(appendentries_reply_encode(&r, &outdata, &szout) < 0){
				netlog("appendentries_reply_encode failed: %r");
				goto closeit;
			}

			if(reply_and_free(fd, outdata, szout) < 0){
				netlog("reply failed: %r");
				goto closeit;
			}

			break;

		case CMD_REQUEST_VOTE:
			;
			VoteRequest vr;
			VoteReply reply;

			rv = vote_request_decode(&vr, data, szin);
			free(data);
			if(rv < 0){
				netlog("vote_request_decode failed: %r");
				goto closeit;
			}

			qlock(rt);
			if(rt->r == nil){
				qunlock(rt);
				netlog("no raft to hand");
				goto closeit;
			}

			consensus_vote(rt->r, &vr, &reply);

			qunlock(rt);

			if(vote_reply_encode(&reply, &outdata, &szout) < 0){
				netlog("vote_reply_encode failed: %r");
				goto closeit;
			}

			if(reply_and_free(fd, outdata, szout) < 0){
				netlog("reply failed: %r");
				goto closeit;
			}
			break;

		default:
			netlog("unknown command recveived: %hud", cmd);
			goto closeit;
		}
	}

closeit:
	close(fd);
}

// process to accept incoming calls from remote peers
static void
_listener(void *v)
{
	RaftTCP *rt;
	int lfd, lfail;
	char ldir[NETPATHLEN];
	int dfd;

	rt = v;

	lfail = 0;
	ldir[0] = 0;

	threadsetname("listener %s\n", rt->addr);

	for(;;){
		lfd = listen(rt->adir, ldir);
		if(lfd < 0){
			fprint(2, "listen: %r");
			if(lfail++ > 10)
				sysfatal("listen: %r");
			continue;
		}

		dfd = accept(lfd, ldir);
		if(dfd < 0){
			fprint(2, "accept: %r");
			if(lfail++ > 10)
				sysfatal("accept: %r");
			continue;
		}

		void **arg;
		arg = raftmalloc(sizeof(void*) * 2);
		arg[0] = rt;
		arg[1] = (void*)dfd;

		proccreate(_incoming, arg, 8192);
	}
}

static int
send_rpc(RaftTCP *rt, s64int peer, u16int cmd, uchar *req, ulong reqlen, uchar **rep, ulong *replen)
{
	RaftTCPPeer *con;
	uchar buf[4+2], *data;
	long sz;

	con = lookupkey(rt->outgoing, peer);
	if(con == nil){
		netlog("no con for peer %lld!", peer);
		return -1;
	}

	PBIT32(buf, reqlen);
	PBIT16(buf+4, cmd);

	qlock(con);
	if(con->fd < 0){
		werrstr("not connected");
		qunlock(con);
		return -1;
	}

	if(iowrite(con->io, con->fd, buf, sizeof(buf)) < 0){
		netlog("send_rpc %s header failed: %r", con->addr);
		goto out;
	}

	if(iowrite(con->io, con->fd, req, reqlen) < 0){
		netlog("send_rpc %s command %hud failed: %r", con->addr, cmd);
		goto out;
	}

	iowerrstr(con->io, "short read");
	if(ioreadn(con->io, con->fd, buf, 4) != 4){
		netlog("send_rpc %s read header failed: %r", con->addr);
		goto out;
	}

	sz = GBIT32(buf);
	data = raftmalloc(sz);

	iowerrstr(con->io, "short read");
	if(ioreadn(con->io, con->fd, data, sz) != sz){
		free(data);
		netlog("send_rpc %s read command %lud reply failed: %r", con->addr, cmd);
		goto out;
	}

	qunlock(con);

	*rep = data;
	*replen = sz;

	return 0;

out:
	/* close and reconnect, poke the connect proc */
	ioclose(con->io, con->fd);
	con->fd = -1;
	rwakeup(&con->r);

	qunlock(con);
	return -1;
}

static int
send_vote(void *v, s64int peer, VoteRequest *vr, VoteReply *r)
{
	RaftTCP *rt;
	uchar *out, *in;
	ulong outlen, inlen;
	int rv;

	assert(v != nil);

	rt = v;

	if(vote_request_encode(vr, &out, &outlen) < 0)
		return -1;

	// TODO: retry?
	rv = send_rpc(rt, peer, CMD_REQUEST_VOTE, out, outlen, &in, &inlen);
	free(out);
	if(rv < 0)
		return -1;

	rv = vote_reply_decode(r, in, inlen);
	free(in);
	if(rv < 0)
		return -1;

	return 0;
}

static int
send_appendentries(void *v, s64int peer, AppendEntriesRequest *ar, AppendEntriesReply *r)
{
	RaftTCP *rt;
	uchar *out, *in;
	ulong outlen, inlen;
	int rv;

	assert(v != nil);

	rt = v;

	if(appendentries_request_encode(ar, &out, &outlen) < 0)
		return -1;

	// TODO: retry?
	rv = send_rpc(rt, peer, CMD_APPEND_ENTRIES, out, outlen, &in, &inlen);
	free(out);
	if(rv < 0)
		return -1;

	rv = appendentries_reply_decode(r, in, inlen);
	free(in);
	if(rv < 0)
		return -1;

	return 0;
}

RPCOps RPCOpsTCP = {
	.requestvote = send_vote,
	.appendentries = send_appendentries,
};
