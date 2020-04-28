#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include "consensus.h"
#include "raftnet.h"

enum {
	CMD_CREATE = 0,
};

static Cmdtab cmdtab[] = {
{
	.index = CMD_CREATE,
	.cmd = "create",
	.narg = 2,
},
};

static int
committed(void *v, CommitEntry *ce)
{
	USED(v);
	Cmdbuf *cb;
	Cmdtab *cmd;
	Srv *fs;
	File *f;

	fs = v;

	fprint(2, "COMMIT index=%lld term=%lld size=%lud cmd=%.*s\n",
		ce->index, ce->term, ce->sz, (int)ce->sz, (char*)ce->cmd);

	cb = parsecmd((char*)ce->cmd, ce->sz);
	assert(cb != nil);

	cmd = lookupcmd(cb, cmdtab, nelem(cmdtab));
	if(cmd == nil){
		fprint(2, "bad command\n");
		return 0;
	}

	switch(cmd->index){
	case CMD_CREATE:
		f = createfile(fs->tree->root, cb->f[1], "raft", 0777, nil);
		if(f == nil){
			fprint(2, "creatfile failed: %r");
			goto out;
		}

		closefile(f);
		break;

	default:
		abort();
	}

out:
	free(cb);
	return 0;
}

static StateMachineOps smops = {
	.committed = committed,
};

static char*
parse_peer(char *opt, s64int *id)
{
	char *c, *dup, *addr;
	s64int tmp;

	dup = strdup(opt);

	c = strchr(dup, ':');
	if(c == nil){
		free(dup);
		return nil;
	}

	*c++ = '\0';

	tmp = strtoul(opt, nil, 10);
	if(tmp == 0){
		return nil;
	}

	addr = strdup(c);
	free(dup);

	*id = tmp;
	return addr;
}

static void
fsopen(Req *r)
{
	respond(r, nil);
}

static void
fscreate(Req *r)
{
	Consensus *c;
	char buf[128];
	int n;

	c = r->srv->aux;

	n = snprint(buf, sizeof(buf), "create %s", r->ifcall.name);

	if(consensus_submit(c, buf, n) < 0){
		responderror(r);
		return;
	}

	respond(r, "not yet");
}

static void
fsread(Req *r)
{
	respond(r, "no");
}

static void
fswrite(Req *r)
{
	respond(r, "no");
}

static void
fsremove(Req *r)
{
	respond(r, "no");
}

Srv fs = {
	.attach = nil,
	.open = fsopen,
	.create = fscreate,
	.read = fsread,
	.write = fswrite,
};

static void
usage(void)
{
	fprint(2, "%s: -l addr -a addr -i id [-d datadir] [-p peerid:peeraddr]\n", argv0);
	sysfatal("usage");
}

void
threadmain(int argc, char *argv[])
{
	StoreDir sd;
	ConsensusSettings cs;
	Consensus *raft;
	RaftTCP *server;
	char *bindaddr, *servaddr;
	s64int id, tmp;
	char *p, *a;
	int i;

	s64int peerids[32];
	char *peeraddrs[32];
	int npeers;

	bindaddr = servaddr = nil;
	id = 0;

	sprint(sd.dir, ".");

	npeers = 0;

	fmtinstall('H', encodefmt);

	ARGBEGIN{
	case 'l':
		servaddr = strdup(EARGF(usage()));
		break;
	case 'a':
		bindaddr = strdup(EARGF(usage()));
		break;
	case 'i':
		id = strtoul(EARGF(usage()), nil, 10);
		break;
	case 'd':
		snprint(sd.dir, sizeof(sd.dir), "%s", EARGF(usage()));
		break;
	case 'p':
		p = EARGF(usage());
		a = parse_peer(p, &tmp);
		if(a == nil)
			usage();

		if(npeers >= sizeof(peerids))
			sysfatal("too many peers");

		peerids[npeers] = tmp;
		peeraddrs[npeers] = a;
		npeers++;

		break;
	}ARGEND

	if(servaddr == nil || bindaddr == nil || id <= 0)
		usage();

	if(npeers % 2 == 0 || npeers < 3)
		fprint(2, "warning: strange peer count\n");

	server = raft_tcp_new(bindaddr);
	if(server == nil)
		sysfatal("server: %r");

	for(i = 0; i < npeers; i++){
		print("peer %lld = %s\n", peerids[i], peeraddrs[i]);
		raft_tcp_add_peer(server, peerids[i], peeraddrs[i]);
	}

	threadsetname("raft peer id %lld", id);

	cs.smops = smops;
	cs.smarg = &fs;
	cs.rops = RPCOpsTCP;
	cs.rarg = server;
	cs.sops = StoreDirOps;
	cs.sarg = &sd;

	raft = consensus_new(id, peerids, npeers, cs);
	if(raft == nil)
		sysfatal("raft init: %r");

	consensus_start(raft);

	raft_tcp_set_server(server, raft);

	fs.tree = alloctree("raft", "raft", DMDIR|0777, nil);
	fs.aux = raft;

	threadlistensrv(&fs, servaddr);

	Ioproc *io = ioproc();
	for(i = 0; ; i++){
		iosleep(io, 1000);

/*
		char buf[128];
		int n;
		n = snprint(buf, sizeof(buf), "x=%d", i);
		consensus_submit(raft, buf, n);
*/
	}

	threadexitsall(nil);
}
