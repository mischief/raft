#include <u.h>
#include <libc.h>
#include <thread.h>

#include "consensus.h"

struct peers {
	Consensus **c;
	
	int n;
};

int
vote(void *v, s64int id, VoteRequest *vr, VoteReply *r)
{
	int i;
	struct peers *p;
	Consensus *c, *ct;

	p = v;
	c = nil;

	for(i = 0; i < p->n; i++){
		ct = p->c[i];
		if(getid(ct) != id)
			continue;
		c = ct;
		break;
	}

	assert(c != nil);

	consensus_vote(c, vr, r);

	return 0;
}

int
append(void *v, s64int id, AppendEntriesRequest *vr, AppendEntriesReply *r)
{
	int i;
	struct peers *p;
	Consensus *c, *ct;

	p = v;
	c = nil;

	for(i = 0; i < p->n; i++){
		ct = p->c[i];
		if(getid(ct) != id)
			continue;
		c = ct;
		break;
	}

	assert(c != nil);

	consensus_append(c, vr, r);

	return 0;
}

int
committed(void *v, CommitEntry *ce)
{
	USED(v);

	fprint(2, "COMMIT index=%lld term=%lld size=%lud cmd=%.*s\n",
		ce->index, ce->term, ce->sz, (int)ce->sz, (char*)ce->cmd);

	return 0;
}

RPCOps rops = {
	.requestvote = vote,
	.appendentries = append,
};

StateMachineOps smops = {
	.committed = committed,
};

void
makepeers(s64int *peers, int npeers, s64int except)
{
	s64int i;
	int n;

	for(i = 1, n = 0; n < npeers; i++){
		if(i == except){
			continue;
		}
		peers[n] = i;
		n++;
	}
}

void
threadmain(int argc, char *argv[])
{
	int i, n;
	s64int peerids[32];
	struct peers p;
	StoreDir sd;
	Ioproc *io;

	ARGBEGIN{
	}ARGEND

	sprint(sd.dir, ".");

	io = ioproc();

	n = 3;

	p.c = malloc(sizeof(*p.c) * n);
	p.n = n;

	for(i = 0; i < n; i++){
		makepeers(peerids, n-1, i+1);
		ConsensusSettings cs = {
			.smops = smops,
			.smarg = &p,
			.rops = rops,
			.rarg = &p,
			.sops = StoreDirOps,
			.sarg = &sd,
		};
		p.c[i] = consensus_new(i+1, peerids, n-1, cs);
	}

	for(i = 0; i < n; i++)
		consensus_start(p.c[i]);

if(1){
	int j;
	for(j = 0; j < 10; j++){
	for(i = 0; ; i = (i + 1) % n){
		iosleep(io, 250);
		fprint(2, "submit to %lld\n", getid(p.c[i]));
		char buf[12];
		sprint(buf, "hello=%d", j);
		if(consensus_submit(p.c[i], buf, strlen(buf)) < 0)
			fprint(2, ">>>> submit failure: %r <<<<\n");
		else {
			fprint(2, ">>>> submit success <<<<\n");
			break;
		}
		yield();
	}
	}
}

	iosleep(io, 500);

	threadexitsall(nil);
}
