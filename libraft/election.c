#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include "consensus.h"
#include "impl.h"

typedef struct Election Election;
struct Election
{
	Consensus *cm;
	Channel *c;
	u64int term, peer;
};

void
_elect(void *v)
{
	int rv;
	Election *e;
	Consensus *cm;
	VoteRequest vr;
	VoteReply r;
	ulong vote;

	e = v;
	cm = e->cm;

	memset(&vr, 0, sizeof(vr));
	memset(&r, 0, sizeof(r));

	vote = 0;

	qlock(cm);

	lastindexandterm(cm, &vr.logindex, &vr.logterm);

	vr.term = e->term;
	vr.candidate = e->cm->id;

	qunlock(cm);

	clog(cm, 2, "VVVV requesting vote from %lld", e->peer);
	rv = cm->settings.rops.requestvote(cm->settings.rarg, e->peer, &vr, &r);
	if(rv != 0){
		clog(cm, 2, "VVVV requestvote=%d %r", rv);
		goto done;
	}

	clog(cm, 2, "VVVV got vote reply term=%lld grant=%llud", r.term, r.grant);

	qlock(cm);
	if(cm->state != CANDIDATE){
		clog(cm, 1, "VVVV white waiting for reply became not candidate");
		qunlock(cm);
		goto done;
	}

	if(r.term > e->term){
		clog(cm, 1, "VVVV term out of date");
		becomefollower(cm, r.term);
	} else if(r.term == e->term && r.grant == 1){
		vote = 1;
	}

	qunlock(cm);

done:
	sendul(e->c, vote);
	free(e);
}

typedef struct Collect Collect;
struct Collect
{
	Consensus *cm;
	Channel *c;
};

void
_votecollector(void *v)
{
	int i;
	ulong votes;
	Collect *c;

	votes = 1;
	c = v;

	clog(c->cm, 2, "VVVV collecting votes: %lud/%d", votes, c->cm->npeers);

	for(i = 0; i < c->cm->npeers; i++){
		votes += recvul(c->c);
		clog(c->cm, 2, "VVVV votes: %lud/%d", votes, c->cm->npeers);
	}

	if(votes*2 > (c->cm->npeers+1)){
		clog(c->cm, 2, "VVVV won election, i am leader");
		startleader(c->cm);
	}

	chanclose(c->c);
	chanfree(c->c);
	free(c);
}

// cm locked
void
startelection(Consensus *cm)
{
	int i;
	u64int savedterm;
	Election *e;
	Collect *co;
	Channel *c;

	cm->state = CANDIDATE;
	cm->term++;

	savedterm = cm->term;

	cm->election = nsec();
	cm->votedFor = cm->id;

	clog(cm, 1, "VVVV beginning election!");

	c = chancreate(sizeof(ulong), 1);

	for(i = 0; i < cm->npeers; i++){
		e = malloc(sizeof(*e));
		e->cm = cm;
		e->c = c;
		e->term = savedterm;
		e->peer = cm->peers[i];
		
		threadcreate(_elect, e, 8192);
	}

	co = malloc(sizeof(*co));
	co->cm = cm;
	co->c = c;

	threadcreate(_votecollector, co, 8192);

	runtimer(cm);
}
