#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include <cbor.h>

#include "consensus.h"
#include "impl.h"
#include "serialize.h"

static int
encode_frob(cbor *cbo, uchar **buf, ulong *sz)
{
	ulong e;
	uchar *out;

	*buf = nil;
	*sz = 0;

	if(cbo == nil)
		return -1;

	e = cbor_encode_size(cbo);
	assert(e != 0);

	out = raftmalloc(e);
	assert(cbor_encode(cbo, out, e) == e);

	*buf = out;
	*sz = e;

	cbor_free(&cbor_default_allocator, cbo);

	return 0;
}

int
vote_request_decode(VoteRequest *vr, uchar *buf, ulong sz)
{
	cbor *cbo;
	int rv = -1;

	cbo = cbor_decode(&cbor_default_allocator, buf, sz);
	if(cbo == nil)
		return -1;

	if(cbor_unpack(&cbor_default_allocator, cbo, "[iiii]",
			&vr->term, &vr->candidate, &vr->logindex, &vr->logterm) < 0)
		goto out;

	rv = 0;

out:
	cbor_free(&cbor_default_allocator, cbo);
	return rv;
}

int
vote_request_encode(VoteRequest *vr, uchar **buf, ulong *sz)
{
	cbor *cbo;

	cbo = cbor_pack(&cbor_default_allocator, "[iiii]",
		vr->term, vr->candidate, vr->logindex, vr->logterm);

	return encode_frob(cbo, buf, sz);
}

int
vote_reply_decode(VoteReply *vr, uchar *buf, ulong sz)
{
	cbor *cbo;
	int rv = -1;

	cbo = cbor_decode(&cbor_default_allocator, buf, sz);
	if(cbo == nil)
		return -1;

	if(cbor_unpack(&cbor_default_allocator, cbo, "[ii]",
			&vr->term, &vr->grant) < 0)
		goto out;

	rv = 0;

out:
	cbor_free(&cbor_default_allocator, cbo);
	return rv;
}

int
vote_reply_encode(VoteReply *vr, uchar **buf, ulong *sz)
{
	cbor *cbo;

	cbo = cbor_pack(&cbor_default_allocator, "[ii]",
		vr->term, vr->grant);

	return encode_frob(cbo, buf, sz);
}

int
appendentries_request_decode(AppendEntriesRequest *aer, uchar *buf, ulong sz)
{
	cbor *cbo, *log;
	LogEntry **entries, *e;
	s64int i, term;
	uchar *cmd;
	int cmdsz;
	int rv = -1;

	cbo = cbor_decode(&cbor_default_allocator, buf, sz);
	if(cbo == nil)
		return -1;

	if(cbor_unpack(&cbor_default_allocator, cbo, "[iiiiic]",
			&aer->term, &aer->leader, &aer->previdx, &aer->prevterm,
			&aer->leadercommit, &log) < 0)
		goto out;

	if(log->type != CBOR_ARRAY)
		goto out;

	aer->nentries = log->len;

	entries = raftmalloc(sizeof(*entries) * aer->nentries);

	for(i = 0; i < log->len; i++){
		if(cbor_unpack(&cbor_default_allocator, log->array[i], "[ib]",
				&term, &cmdsz, &cmd) < 0){
			free(entries);
			goto out;
		}

		e = newlog(cmd, cmdsz);
		e->term = term;
		entries[i] = e;
		free(cmd);
	}

	aer->entries = entries;

	rv = 0;

out:
	cbor_free(&cbor_default_allocator, cbo);
	return rv;
}

int
appendentries_request_encode(AppendEntriesRequest *aer, uchar **buf, ulong *sz)
{
	LogEntry *e;
	cbor *cbo, *log, *entry;
	int i;

	log = cbor_make_array(&cbor_default_allocator, aer->nentries);
	if(log == nil)
		return -1;

	for(i = 0; i < aer->nentries; i++){
		e = aer->entries[i];
		if(e->term > (2LL<<16LL) || e->term < -(2LL<<16LL))
			abort();
		entry = cbor_pack(&cbor_default_allocator, "[ib]",
			e->term, e->sz, e->cmd);
		if(entry == nil)
			goto errlog;
		log->array[i] = entry;
	}

	cbo = cbor_pack(&cbor_default_allocator, "[iiiiic]",
			aer->term, aer->leader, aer->previdx, aer->prevterm,
			aer->leadercommit, log);

	return encode_frob(cbo, buf, sz);

errlog:
	cbor_free(&cbor_default_allocator, log);
	return -1;
}

int
appendentries_reply_decode(AppendEntriesReply *aer, uchar *buf, ulong sz)
{
	cbor *cbo;
	int rv = -1;

	cbo = cbor_decode(&cbor_default_allocator, buf, sz);
	if(cbo == nil)
		return -1;

	if(cbor_unpack(&cbor_default_allocator, cbo, "[ii]",
			&aer->term, &aer->success) < 0)
		goto out;

	rv = 0;

out:
	cbor_free(&cbor_default_allocator, cbo);
	return rv;
}

int
appendentries_reply_encode(AppendEntriesReply *aer, uchar **buf, ulong *sz)
{
	cbor *cbo;

	cbo = cbor_pack(&cbor_default_allocator, "[ii]",
		aer->term, aer->success);

	return encode_frob(cbo, buf, sz);
}
