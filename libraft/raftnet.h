typedef struct RaftTCP RaftTCP;

#pragma incomplete RaftTCP

RaftTCP *raft_tcp_new(char *addr);
void raft_tcp_add_peer(RaftTCP *rt, s64int peer, char *addr);
void raft_tcp_set_server(RaftTCP *rt, Consensus *c);

extern RPCOps RPCOpsTCP;
