#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include <cbor.h>

#include "consensus.h"
#include "impl.h"

static char*
cbor_print(cbor *c, char *bp, char *be)
{
	int i;
	char *p, *e, *bracket;

	fmtinstall('H', encodefmt);

	bp = seprint(bp, be, "(type=%d) ", c->type);

	switch(c->type){
	case CBOR_UINT:
		return seprint(bp, be, "%llud", c->uint);

	case CBOR_NINT:
		return seprint(bp, be, "-%llud", c->uint);

	case CBOR_BYTE:
		return seprint(bp, be, "%.*H", c->len, c->byte);

	case CBOR_STRING:
		return seprint(bp, be, "\"%.*s\"", c->len, c->string);

	case CBOR_ARRAY:
		bracket = "[]";
		goto arr;

	case CBOR_MAP:
		bracket = "{}";

arr:
		p = bp;
		e = be;

		p = seprint(p, e, "%c", bracket[0]);

		for(i = 0; i < c->len; i++){
			p = cbor_print(c->array[i], p, e);
			if(c->len > 1 && i < c->len - 1)
				p = seprint(p, e, ", ");
		}

		p = seprint(p, e, "%c", bracket[1]);
		return p;

	case CBOR_MAP_ELEMENT:
		p = bp;
		e = be;

		p = cbor_print(c->key, p, e);
		p = seprint(p, e, ": ");
		p = cbor_print(c->value, p, e);
		return p;

	case CBOR_TAG:
		p = bp;
		e = be;

		p = seprint(p, e, "%llud(", c->tag);
		p = cbor_print(c->item, p, e);
		p = seprint(p, e, ")");
		return p;

	case CBOR_NULL:
		return seprint(bp, be, "null");

	case CBOR_FLOAT:
		return seprint(bp, be, "%#g", c->f);

	case CBOR_DOUBLE:
		return seprint(bp, be, "%#g", c->d);

	default:
		abort();
	}

	return nil;
}

void
cbor_fprint(int fd, cbor *c)
{
	char buf[2048];

	cbor_print(c, buf, buf+sizeof(buf));

	fprint(fd, "%s\n", buf);
}

enum {
	STORE_V1 = 1,
};

static cbor*
serialize_log_entry(Consensus *cm, s64int index)
{
	LogEntry *e;
	cbor *cbo;

	e = getlogentry(cm, index);
	assert(e != nil);

	cbo = cbor_pack(&cbor_default_allocator, "[ib]", e->term, (int)e->sz, e->cmd);

//	fprint(2, "serialize_log_entry: ");
//	cbor_fprint(2, cbo);

	freelogentry(e);

	return cbo;
}

static cbor*
serialize_log(Consensus *cm)
{
	s64int i;
	cbor *log, *tmp;

	log = cbor_make_array(&cbor_default_allocator, cm->nlog);
	if(log == nil)
		return nil;

	for(i = 0; i < cm->nlog; i++){
		tmp = serialize_log_entry(cm, i);
		if(tmp == nil)
			goto err;
		log->array[i] = tmp;
	}

	return log;

err:
	cbor_free(&cbor_default_allocator, log);
	return nil;
}

// cm locked
int
raft_save(Consensus *cm)
{
	cbor *data, *log;
	s64int version = STORE_V1;

	assert(canqlock(cm) == 0);

	clog(cm, 3, "serializing data");

	log = serialize_log(cm);
	if(log == nil)
		return -1;

	data = cbor_pack(&cbor_default_allocator, "[iiic]", version, cm->term, cm->votedFor, log);
	if(data == nil){
		cbor_free(&cbor_default_allocator, log);
		return -1;
	}

	ulong sz;
	sz = cbor_encode_size(data);
	uchar *buf = malloc(sz);
	if(buf == nil)
		goto err;

	if(cbor_encode(data, buf, sz) != sz)
		sysfatal("unexpected encode size");


	cbor_free(&cbor_default_allocator, data);

	char fname[32];
	snprint(fname, sizeof(fname), "raftlog.%lld", cm->id);

	if(cm->settings.sops.set(cm->settings.sarg, fname, buf, sz) < 0)
		sysfatal("cant write raft data: %r");

	free(buf);

	clog(cm, 3, "serialization complete");
	return 0;

err:
	cbor_free(&cbor_default_allocator, data);
	return -1;
}

int
raft_load(Consensus *cm)
{
	cbor *cbo;
	uchar *buf;
	int sz;

	assert(canqlock(cm) == 0);

	char fname[32];
	snprint(fname, sizeof(fname), "raftlog.%lld", cm->id);

	clog(cm, 1, "deserializing data from %s", fname);

	if(cm->settings.sops.get(cm->settings.sarg, fname, &buf, &sz) < 0){
		clog(cm, 1, "no data loaded from %s: %r", fname);
		return -1;
	}

	fmtinstall('H', encodefmt);
	clog(cm, 3, "decoding cbor: sz=%d %.*H", sz, sz, buf);

	cbo = cbor_decode(&cbor_default_allocator, buf, sz);
	free(buf);
	if(cbo == nil){
		clog(cm, 1, "bad cbor data in %s: %r", fname);
		return -1;
	}

	//fprint(2, "%d %d\n", cbo->array[0]->type, CBOR_SINT);

	if(cbo->type != CBOR_ARRAY || cbo->len < 1 /*||
			cbo->array[0]->type != CBOR_SINT || cbo->array[0]->sint != STORE_V1*/){
		clog(cm, 1, "unexpected cbor data in %s", fname);
		goto err;
	}

	//cbor_fprint(2, cbo);

	s64int version, term, votedFor;
	cbor *log;

	if(cbor_unpack(&cbor_default_allocator, cbo, "[iiic]", &version, &term, &votedFor, &log) < 0){
		clog(cm, 1, "cbor unpack failure in %s", fname);
		goto err;
	}

	if(version != STORE_V1)
		sysfatal("incompatible version in stored data");

	if(log->type != CBOR_ARRAY){
		clog(cm, 1, "log data not array!");
		goto err;
	}

	s64int index;
	for(index = 0; index < log->len; index++){
		LogEntry *e;
		s64int term;
		uchar *cmd;
		int sz;

		if(cbor_unpack(&cbor_default_allocator, log->array[index], "[ib]", &term, &sz, &cmd) < 0)
			sysfatal("bad log entry at index %lld", index);

		e = newlog(cmd, sz);
		e->term = term;
		assert(insertkey(cm->log, index, e) == nil);
		free(cmd);
	}

	cm->nlog = log->len;
	cm->term = term;
	cm->votedFor = votedFor;

	clog(cm, 1, "deserialization complete");
	cbor_free(&cbor_default_allocator, cbo);
	return 0;

err:
	cbor_free(&cbor_default_allocator, cbo);
	return -1;
}

static int
rename(int fd, char *name)
{
	Dir d;
	nulldir(&d);
	d.name = name;
	int rv = dirfwstat(fd, &d);

	if(rv < 0){
		fprint(2, "really rename to %s: %r\n", name);
		return -1;
	}

	return 0;
}

static int
swapnames(int fd, char *name)
{
	Dir d;
	char buf[128], dir[128], backup[128], argh[128];

	nulldir(&d);

	if(fd2path(fd, dir, sizeof(dir)) < 0)
		return -1;

	*strrchr(dir, '/') = 0;

	snprint(buf, sizeof(buf), "%s/%s", dir, name);

	if(access(buf, AEXIST) == 0){
		snprint(backup, sizeof(backup), "%s.backup", name);
		snprint(argh, sizeof(argh), "%s/%s", dir, backup);
		(void) remove(argh);
		d.name = backup;

		if(dirwstat(buf, &d) < 0)
			return -1;
	}

	return rename(fd, name);
}

void
tmpname(char *buf, int len)
{
	int tid, pid;

	tid = threadid();
	pid = threadpid(tid);

	snprint(buf, len, "rafttmp.%d.%d.%ld", pid, tid, lrand());
}

static int
set_dir(void *v, char *key, uchar *value, int sz)
{
	StoreDir *d;
	int fd, rv;
	char *dir;
	char tmpnam[128], tmpfile[512];

	d = v;
	dir = d->dir;

	tmpname(tmpnam, sizeof(tmpnam));
	snprint(tmpfile, sizeof(tmpfile), "%s/%s", dir, tmpnam);
	cleanname(tmpfile);

	fd = create(tmpfile, OWRITE|OTRUNC, 0644);
	if(fd < 0)
		return -1;

	if(write(fd, value, sz) != sz)
		return -1;

	rv = swapnames(fd, key);
	close(fd);
	if(rv < 0)
		return -1;

	return 0;
}

static int
get_dir(void *v, char *key, uchar **value, int *sz)
{
	StoreDir *sd;
	int fd;
	char filename[512];
	Dir *d;
	vlong len;
	uchar *buf;

	sd = v;

	*value = nil;
	*sz = 0;

	snprint(filename, sizeof(filename), "%s/%s", sd->dir, key);
	cleanname(filename);

	fd = open(filename, OREAD);
	if(fd < 0)
		goto err;

	d = dirfstat(fd);
	if(d == nil)
		goto err;

	len = d->length;
	free(d);

	buf = malloc(len);
	if(buf == nil)
		goto err;

	werrstr("short read");
	if(readn(fd, buf, len) != len){
		free(buf);
		return -1;
	}

	*value = buf;
	*sz = len;

	return 0;

err:
	if(fd >= 0)
		close(fd);
	return -1;
}

StoreOps StoreDirOps = {
	.set = set_dir,
	.get = get_dir,
};
