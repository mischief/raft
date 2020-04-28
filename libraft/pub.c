#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include "consensus.h"
#include "impl.h"

Consensus*
consensus_new(s64int id, s64int *peers, int npeers, ConsensusSettings settings)
{
	int i;
	char buf[128], *p, *e;
	Consensus *c;

	c = malloc(sizeof(*c));
	if(c == nil)
		return nil;

	memset(c, 0, sizeof(*c));

	c->settings = settings;

	c->id = id;

	assert(npeers <= 32);
	memcpy(c->peers, peers, sizeof(u64int)*(npeers));
	c->npeers = npeers;

	c->commitready = chancreate(sizeof(ulong), 16);

	c->dbglevel = 1;

	c->state = FOLLOWER;
	c->votedFor = -1;

	c->log = allocmap(logincref);
	c->nlog = 0;

	c->commit = -1;
	c->applied = -1;

	c->election = nsec();

	c->nextindex = allocmap(nil);
	c->matchindex = allocmap(nil);

	p = buf;
	e = buf+sizeof(buf);
	*p = 0;

	for(i = 0; i < c->npeers; i++)
		p = seprint(p, e, " %lld", c->peers[i]);

	clog(c, 1, "new: npeers=%d:%s", c->npeers, buf);

	qlock(c);
	if(raft_load(c) < 0)
		clog(c, 1, "raft_load failed: %r");
	qunlock(c);

	return c;
}

void
consensus_start(Consensus *cm)
{
	clog(cm, 2, "start");
	runtimer(cm);
	commitprocessor(cm);
}

void
consensus_stop(Consensus *cm)
{
	clog(cm, 2, "stop");
	assert(0);
}

void
consensus_vote(Consensus *cm, VoteRequest *vr, VoteReply *r)
{
	s64int index, term;

	clog(cm, 2, "rpc: vote: term=%lld candidate=%lld logindex=%lld logterm=%lld",
		vr->term, vr->candidate, vr->logindex, vr->logterm);

	qlock(cm);

	if(cm->state == DEAD)
		goto done;

	lastindexandterm(cm, &index, &term);

	if(vr->term > cm->term){
		clog(cm, 2, "rpc: vote: term out of date when asked for vote");
		becomefollower(cm, vr->term);
	}

	if(vr->term == cm->term &&
			(cm->votedFor == -1 || cm->votedFor == vr->candidate) &&
			(vr->logterm > term || (vr->logterm == term && vr->logindex >= index))){
		r->grant = 1;
		cm->votedFor = vr->candidate;
		cm->election = nsec();
	} else {
		r->grant = 0;
	}

	r->term = cm->term;

	assert(raft_save(cm) == 0);

	clog(cm, 2, "rpc: vote: reply to candidate=%lld term=%lld grant=%lld",
		vr->candidate, r->term, r->grant);

done:
	qunlock(cm);
}

static s64int
s64min(s64int a, s64int b)
{
	if(a < b)
		return a;
	return b;
}

void
consensus_append(Consensus *cm, AppendEntriesRequest *ar, AppendEntriesReply *r)
{
	qlock(cm);

	if(cm->state == DEAD)
		goto done;

	clog(cm, 2, "appendentries nentries=%d leader=%lld", ar->nentries, ar->leader);

	if(ar->term > cm->term){
		clog(cm, 2, "term out of date in appendentries");
		becomefollower(cm, ar->term);
		goto done;
	}

	r->success = 0;

	if(ar->term == cm->term){
		if(cm->state != FOLLOWER){
			becomefollower(cm, ar->term);
		}
		cm->election = nsec();

		clog(cm, 3, "append: ar->previndex %lld cm->nlog %lld ar->prevterm %lld",
			ar->previdx, cm->nlog, ar->prevterm);

		u64int prevlogterm;

		if(ar->previdx > -1 && ar->previdx < cm->nlog)
			prevlogterm = getlogterm(cm, ar->previdx);
		else
			prevlogterm = 0;

		clog(cm, 3, "append: prevlogterm %lld", prevlogterm);

		if(ar->previdx == -1 ||
				(ar->previdx < cm->nlog && ar->prevterm == prevlogterm)){
			r->success = 1;

			/* update leader */
			cm->leader = ar->leader;

			s64int insert = ar->previdx + 1;
			s64int newidx = 0;

			clog(cm, 3, "start insert=%lld newidx=%lld", insert, newidx);

			for(;;){
				if(insert >= cm->nlog || newidx >= ar->nentries)
					break;
				if(getlogterm(cm, insert) != ar->entries[newidx]->term)
					break;
				insert++;
				newidx++;
			}

			clog(cm, 3, "end insert=%lld newidx=%lld", insert, newidx);
			clog(cm, 3, "ar->nentries = %d", ar->nentries);

			if(newidx < ar->nentries){
				// commit ar->entries to insert+1;
				LogEntry *e, *old;

				for(; newidx < ar->nentries; newidx++){
					if(ar->entries[newidx]->term > (2LL<<32LL)){
						clog(cm, 1, "suspicious log entry");
						abort();
					}

					e = logdup(ar->entries[newidx]);
					clog(cm, 3, "inserting log[%lld]=ar[%lld] term %lld", insert, newidx, e->term);
					old = insertkey(cm->log, insert++, e);
					if(old != nil)
						freelogentry(old);
					else
						cm->nlog++;
				}
			}

			if(ar->leadercommit > cm->commit){
				cm->commit = s64min(ar->leadercommit, cm->nlog-1);
				clog(cm, 2, "... setting commit=%lld", cm->commit);
				sendul(cm->commitready, 0);
			}
		}
	}

	r->term = cm->term;

	assert(raft_save(cm) == 0);

	clog(cm, 2, "appendentries reply term=%lld success=%lld", r->term, r->success);

done:
	qunlock(cm);
}

// cm locked
static void
appendlog(Consensus *cm, void *buf, usize sz)
{
	LogEntry *entry;

	entry = newlog(buf, sz);
	entry->term = cm->term;

	insertkey(cm->log, cm->nlog++, entry);

	clog(cm, 3, "appendlog: nlog=%d term=%lld", cm->nlog, entry->term);

	assert(raft_save(cm) == 0);
}

int
consensus_submit(Consensus *cm, void *buf, usize sz)
{
	int rv;

	rv = -1;

	qlock(cm);

	if(cm->state == LEADER){
		appendlog(cm, buf, sz);
		rv = 0;
	} else {
		/* TODO: relay to leader */
		werrstr("not leader");
	}

	qunlock(cm);
	return rv;
}
