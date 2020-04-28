#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include "consensus.h"
#include "impl.h"

void
logincref(void *v)
{
	LogEntry *e = v;

	incref(e);
}

void
freelogentry(void *v)
{
	LogEntry *e = v;
	if(decref(e) == 0){
		free(e->cmd);
		free(e);
	}
}

LogEntry*
newlog(void *buf, usize sz)
{
	LogEntry *e;

	e = malloc(sizeof(*e));
	if(e == nil)
		sysfatal("out of memory: %r");

	e->cmd = malloc(sz);
	if(e->cmd == nil)
		sysfatal("out of memory: %r");

	e->sz = sz;

	memcpy(e->cmd, buf, sz);

	incref(e);

	return e;
};

LogEntry*
logdup(LogEntry *e)
{
	LogEntry *r;

	r = raftmalloc(sizeof(*r));

	setmalloctag(r, getcallerpc(&e));

	r->term = e->term;
	r->cmd = raftmalloc(e->sz);

	r->sz = e->sz;

	memcpy(r->cmd, e->cmd, r->sz);

	incref(r);

	return r;
}

LogEntry*
getlogentry(Consensus *cm, s64int index)
{
	LogEntry *e;

	e = lookupkey(cm->log, (ulong)index);
	assert(e != nil);
	return e;
}

u64int
getlogterm(Consensus *cm, s64int index)
{
	u64int term;
	LogEntry *e = getlogentry(cm, index);
	term = e->term;
	freelogentry(e);
	return term;
}
