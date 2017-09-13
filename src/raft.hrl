%% The timeout interval after which the leader will send heartbeats.
-define(HEARTBEAT_TIMEOUT, 20).

%% The maximum timeout for nodes (see TIMEOUT_SEED).
-define(TIMEOUT_RANGE, 150).

-ifdef(TEST).

%% The minimum timeout range. This combines with TIMEOUT_RANGE to give
%% a lower and upper limit between which random timeouts are
%% generated.
-define(TIMEOUT_SEED, 15000000000).

%% The list of nodes in the cluster.
-define(NODES, [
                #raft_node{name = n1},
                #raft_node{name = n2},
                #raft_node{name = n3},
                #raft_node{name = n4},
                #raft_node{name = n5}
               ]).

-else.

-define(TIMEOUT_SEED, 150).

-define(NODES, [
                #raft_node{name = n1},
                #raft_node{name = n2},
                #raft_node{name = n3},
                #raft_node{name = n4},
                #raft_node{name = n5}
               ]).

-endif.

-record(vote_request, {term, candidate_id}).

-type vote_request() :: #vote_request{}.


-record(vote_granted, {term, voter_id}).


-record(append_entries, {
            term           :: non_neg_integer(),
            leader_id      :: atom(),
            prev_log_index :: undefined | non_neg_integer(),
            prev_log_term  :: undefined | non_neg_integer(),
            entries = []   :: [log_entry()]
           }).

-type append_entries() :: #append_entries{}.


-record(raft_node, {
            name             :: atom(),
            next_index = 0   :: non_neg_integer(),
            match_index = 0  :: non_neg_integer()
           }).

-type raft_node() :: #raft_node{}.


-record(log_entry, {index, term, command}).

-type log_entry() :: #log_entry{}.


-record(client_message, {
            client_id    :: pid(),
            message_id   :: string(),
            command = "" :: string()
           }).

-type client_message() :: #client_message{}.


-record(metadata, {
            name                 :: atom(),
            nodes = []           :: [raft_node()],
            term = 0             :: non_neg_integer(),
            votes = []           :: [atom()],
            voted_for = null     :: atom(),
            leader_id = null     :: atom(),
            log = []             :: [log_entry()],
            to_reply = []        :: [client_message()]
           }).

-type metadata() :: #metadata{}.
