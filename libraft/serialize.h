int vote_request_decode(VoteRequest *vr, uchar *buf, ulong sz);
int vote_request_encode(VoteRequest *vr, uchar **buf, ulong *sz);
int vote_reply_decode(VoteReply *vr, uchar *buf, ulong sz);
int vote_reply_encode(VoteReply *vr, uchar **buf, ulong *sz);
int appendentries_request_decode(AppendEntriesRequest *aer, uchar *buf, ulong sz);
int appendentries_request_encode(AppendEntriesRequest *aer, uchar **buf, ulong *sz);
int appendentries_reply_decode(AppendEntriesReply *aer, uchar *buf, ulong sz);
int appendentries_reply_encode(AppendEntriesReply *aer, uchar **buf, ulong *sz);
