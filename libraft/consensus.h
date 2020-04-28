typedef struct Consensus Consensus;
typedef struct ConsensusSettings ConsensusSettings;
typedef struct StateMachineOps StateMachineOps;
typedef struct RPCOps RPCOps;
typedef struct StoreOps StoreOps;
typedef struct VoteRequest VoteRequest;
typedef struct VoteReply VoteReply;
typedef struct LogEntry LogEntry;
typedef struct CommitEntry CommitEntry;
typedef struct AppendEntriesRequest AppendEntriesRequest;
typedef struct AppendEntriesReply AppendEntriesReply;

#pragma incomplete Consensus

Consensus* consensus_new(s64int id, s64int *peers, int npeers, ConsensusSettings settings);
void consensus_start(Consensus *cm);
void consensus_stop(Consensus *cm);

/* rpc endpoints */
void consensus_vote(Consensus *cm, VoteRequest *vr, VoteReply *r);
void consensus_append(Consensus *cm, AppendEntriesRequest *ar, AppendEntriesReply *r);
u64int getid(Consensus *cm);

/* command submission entrypoint */
int consensus_submit(Consensus *cm, void *buf, usize sz);

struct StateMachineOps
{
	int (*committed)(void*, CommitEntry*);
};

struct RPCOps
{
	int (*requestvote)(void*, s64int, VoteRequest*, VoteReply*);
	int (*appendentries)(void*, s64int, AppendEntriesRequest*, AppendEntriesReply*);
};

struct StoreOps
{
	int (*set)(void*, char* key, uchar *value, int sz);
	int (*get)(void*, char *key, uchar **value, int *sz);
};

struct ConsensusSettings
{
	StateMachineOps smops;
	void *smarg;

	RPCOps rops;
	void *rarg;

	StoreOps sops;
	void *sarg;
};

struct VoteRequest
{
	s64int term;
	s64int candidate;
	s64int logindex;
	s64int logterm;
};

struct VoteReply
{
	s64int term;

	// 0 or 1
	u64int grant;
};

struct LogEntry
{
	Ref;
	s64int term;
	usize sz;
	uchar *cmd;
};

struct CommitEntry
{
	s64int index;
	s64int term;
	usize sz;
	uchar *cmd;
};

struct AppendEntriesRequest
{
	s64int term;
	s64int leader;

	s64int previdx;
	s64int prevterm;
	s64int leadercommit;

	LogEntry **entries;
	s64int nentries;
};

struct AppendEntriesReply
{
	s64int term;
	s64int success;
};

typedef struct StoreDir StoreDir;
struct StoreDir
{
	char dir[512];
};

extern StoreOps StoreDirOps;
