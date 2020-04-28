enum {
	DEAD = 0,
	FOLLOWER,
	LEADER,
	CANDIDATE,
};

#define timerscale 5

struct Consensus
{
	QLock;

	// unchaging

	ConsensusSettings settings;

	s64int id;
	s64int peers[32];
	int npeers;

	Channel *commitready; // (ulong)

	// changing
	int dbglevel;

	s64int term;				// current term
	s64int votedFor;			// id of peer voted for
	s64int leader;				// current leader

	Intmap *log;				// ulong -> LogEntry*
	s64int nlog;

	s64int commit;				// commit index
	s64int applied;
	s64int state;

	vlong election;				// timestamp of election in ns

	// volatile raft state on leaders
	Intmap *nextindex;
	Intmap *matchindex;
};

void clog(Consensus *cm, int level, char *fmt, ...);
void runtimer(Consensus *cm);
void commitprocessor(Consensus *cm);
void becomefollower(Consensus *cm, s64int term);
void startleader(Consensus *cm);
void startelection(Consensus *cm);
void lastindexandterm(Consensus *cm, s64int *index, s64int *term);

void appendentries_free(AppendEntriesRequest *aer);

void *raftmalloc(ulong);

// log.c
void logincref(void *v);
void freelogentry(void *v);
LogEntry* newlog(void *buf, usize sz);
LogEntry* logdup(LogEntry *e);
LogEntry* getlogentry(Consensus *cm, s64int index);
u64int getlogterm(Consensus *cm, s64int index);

// storage.c
int raft_save(Consensus *cm);
int raft_load(Consensus *cm);
