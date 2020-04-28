#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include "consensus.h"
#include "impl.h"

u64int
getid(Consensus *cm)
{
	u64int id;
	id = cm->id;
	return id;
}

void
clog(Consensus *cm, int level, char *fmt, ...)
{
	va_list arg;
	char *pre, *ldr, *tmp;

	if(level > cm->dbglevel)
		return;

	va_start(arg, fmt);

	ldr = "";
	if(cm->id == cm->leader)
		ldr = "(leader) ";
	pre = smprint("%lld: %s", cm->id, ldr);
	tmp = smprint("%s%s\n", pre, fmt);
	vfprint(2, tmp, arg);
	free(pre);
	free(tmp);
	va_end(arg);
}

// cm locked
void
lastindexandterm(Consensus *cm, s64int *index, s64int *term)
{
	s64int idx;

	if(cm->nlog > 0){
		idx = cm->nlog - 1;
		*index = idx;
		*term = getlogterm(cm, idx);
		return;
	}

	*index = -1;
	*term = -1;
}

// cm locked
void
becomefollower(Consensus *cm, s64int term)
{
	clog(cm, 1, "now follower term=%lld", term);

	cm->state = FOLLOWER;
	cm->term = term;
	cm->votedFor = -1;
	cm->election = nsec();

	runtimer(cm);
}

typedef struct Heartbeat Heartbeat;
struct Heartbeat
{
	Consensus *cm;
	s64int term;
	s64int peer;
};

void
_heartbeat(void *v)
{
	Heartbeat *h;
	Consensus *cm;
	AppendEntriesRequest vr;
	AppendEntriesReply r;
	LogEntry *entry, **entries;
	s64int i, nextindex, prevlogindex, prevlogterm;
	int nentry;

	h = v;
	cm = h->cm;

	threadsetname("heartbeat to %lld", h->peer);

	memset(&vr, 0, sizeof(vr));
	memset(&r, 0, sizeof(r));

	qlock(cm);

	nextindex = (s64int) lookupkey(cm->nextindex, h->peer);
	prevlogindex = nextindex - 1;
	prevlogterm = -1;

	if(prevlogindex >= 0)
		prevlogterm = getlogterm(cm, prevlogindex);

	entries = nil;
	nentry = 0;

	clog(cm, 3, "HHHH to %lld nextindex=%lld", h->peer, nextindex);

	if(nextindex < cm->nlog){
		nentry = cm->nlog - nextindex; // ???
		assert(nentry + nextindex <= cm->nlog);
		entries = raftmalloc(sizeof(LogEntry*) * nentry);

		for(i = 0; i < nentry; i++){
			entry = getlogentry(cm, nextindex+i);
			entries[i] = logdup(entry);
			freelogentry(entry);
		}
	}

	vr.term = h->term;
	vr.leader = cm->id;
	vr.previdx = prevlogindex;
	vr.prevterm = prevlogterm;
	vr.entries = entries;
	vr.nentries = nentry;
	vr.leadercommit = cm->commit;

	qunlock(cm);

	int rv;
	rv = cm->settings.rops.appendentries(cm->settings.rarg, h->peer, &vr, &r);

	appendentries_free(&vr);

	if(rv != 0){
		clog(cm, 1, "HHHH sending appendentries to %lld failed: %r", h->peer);
		goto done;
	}

	clog(cm, 2, "HHHH sending appendentries to %lld success", h->peer);

	qlock(cm);

	if(r.term > h->term){
		clog(cm, 2, "HHHH term out of date in heartbeat reply");
		goto unlockit;
	}

	if(cm->state == LEADER && h->term == r.term){
		if(r.success){
			clog(cm, 3, "HHHH set nextindex[%lld] = %lld", h->peer, nextindex+nentry);
			insertkey(cm->nextindex, h->peer, (void*) (nextindex + nentry));
			nextindex = (s64int) lookupkey(cm->nextindex, h->peer);
			insertkey(cm->matchindex, h->peer, (void*) (nextindex - 1));

			clog(cm, 2, "HHHH appendentries reply from %lld success", h->peer);

			int j;
			s64int i, saved = cm->commit;
			for(i = cm->commit + 1; i < cm->nlog; i++){
				if(getlogterm(cm, i) == cm->term){
					int match = 1;
					for(j = 0; j < cm->npeers; j++){
						s64int index = (s64int) lookupkey(cm->matchindex, h->peer);
						if(index >= i)
							match++;
					}
					clog(cm, 3, "HHHH match=%d", match);
					if(match*2 > cm->npeers+1)
						cm->commit = i;
				}
			}

			clog(cm, 3, "HHHH commit=%lld saved=%lld", cm->commit, saved);

			if(cm->commit != saved){
				clog(cm, 2, "HHHH leader set commit index = %lld", cm->commit);
				sendul(cm->commitready, 0);
			}
		} else {
			insertkey(cm->nextindex, h->peer, (void*) (nextindex - 1));
			clog(cm, 2, "HHHH appendentries reply from %lld !success: nextindex=%lld",
				h->peer, nextindex - 1);
		}
	}

unlockit:
	qunlock(cm);

done:
	free(h);
}

void
heartbeat(Consensus *cm)
{
	int i;
	Heartbeat *h;

	qlock(cm);

	for(i = 0; i < cm->npeers; i++){
		h = malloc(sizeof(*h));
		assert(h != nil);
		h->cm = cm;
		h->term = cm->term;
		h->peer = cm->peers[i];
		threadcreate(_heartbeat, h, 8192);
	}

	qunlock(cm);
}

// cm locked
void
_leader(void *v)
{
	Consensus *cm;
	Ioproc *timer;

	cm = v;

	qunlock(cm);

	timer = ioproc();

	for(;;){
		heartbeat(cm);
		iosleep(timer, 50*timerscale);

		qlock(cm);
		if(cm->state != LEADER){
				qunlock(cm);
				break;
		}

		qunlock(cm);
	}

	closeioproc(timer);
}

void
startleader(Consensus *cm)
{
	int i;

	qlock(cm);
	cm->state = LEADER;
	cm->leader = cm->id;

	for(i = 0; i < cm->npeers; i++){
		insertkey(cm->nextindex, cm->peers[i], (void*) cm->nlog);
		insertkey(cm->matchindex, cm->peers[i], (void*) -1);
	}

	clog(cm, 1, "LLLL become leader id=%lld term=%lld", cm->id, cm->term);

	threadcreate(_leader, cm, 8192);
}

void
_runtimer(void *v)
{
	Consensus *cm;
	long electionTimeout;
	u64int term;
	Ioproc *timer;
	vlong since;

	cm = v;

	electionTimeout = lnrand(150)+150;
	electionTimeout *= timerscale;

	qlock(cm);
	term = cm->term;
	qunlock(cm);

	timer = ioproc();

	clog(cm, 1, "TTTT election timer started (%ld) term %lld", electionTimeout, term);

	for(;;){
		iosleep(timer, 10*timerscale);

		qlock(cm);

		clog(cm, 2, "TTTT election timer wakeup state=%lld term=%lld", cm->state, cm->term);

		if(cm->state != CANDIDATE && cm->state != FOLLOWER)
			goto done;

		if(term != cm->term)
			goto done;

		since = nsec() - cm->election;
		if(since > ((vlong)electionTimeout * 1000000LL)){
			clog(cm, 2, "TTTT it has been %lld ms since i heard from the leader!",
				since/1000000LL);
			startelection(cm);
			goto done;
		}

		qunlock(cm);
	}

done:
	qunlock(cm);
	closeioproc(timer);
}

void
runtimer(Consensus *cm)
{
	threadcreate(_runtimer, cm, 8192);
}

void
_commitprocessor(void *v)
{
	Consensus *cm;
	LogEntry **le, *lp;
	CommitEntry ce;
	ulong u;
	u64int term;
	s64int applied, base, nentries;
	int i;

	cm = v;

	threadsetname("%lld commit processor", cm->id);

	for(;;){
		if(recv(cm->commitready, &u) < 0)
			break;

		clog(cm, 2, "CCCC commit processor wakeup");

		qlock(cm);

		term = cm->term;
		applied = cm->applied;

		le = nil;
		nentries = 0;

		clog(cm, 3, "CCCC cm->applied=%lld cm->commit=%lld",
			cm->applied, cm->commit);

		if(cm->commit > cm->applied){
			nentries = (cm->commit+1) - (cm->applied+1);
			le = malloc(sizeof(LogEntry*) * nentries);
			for(i = 0, base = cm->applied+1; base < (cm->commit+1); i++, base++){
				lp = getlogentry(cm, base);
				le[i] = logdup(lp);
				freelogentry(lp);
			}
			cm->applied = cm->commit;
		}

		clog(cm, 3, "CCCC cm->applied=%lld cm->commit=%lld",
			cm->applied, cm->commit);
		clog(cm, 3, "CCCC entries=%lld, applied=%lld", nentries, applied);

		qunlock(cm);

		for(i = 0; i < nentries; i++){
			lp = le[i];
			ce.index = applied + i + 1;
			ce.term = term;
			ce.sz = lp->sz;
			ce.cmd = lp->cmd;
			cm->settings.smops.committed(cm->settings.smarg, &ce);
			freelogentry(lp);
		}

		free(le);
	}

	clog(cm, 1, "commit processor died");
}

void
commitprocessor(Consensus *cm)
{
	threadcreate(_commitprocessor, cm, 8192);
}

void*
raftmalloc(ulong sz)
{
	void *v;

	v = malloc(sz);
	if(v == nil){
		abort();
		sysfatal("malloc: %r");
	}

	memset(v, 0, sz);

	return v;
}

void
appendentries_free(AppendEntriesRequest *aer)
{
	int i;

	if(aer == nil || aer->entries == nil)
		return;

	for(i = 0; i < aer->nentries; i++)
		freelogentry(aer->entries[i]);

	free(aer->entries);
	aer->entries = nil;
}