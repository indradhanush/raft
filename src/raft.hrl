-record(raft_node, {
            name             :: atom(),
            next_index = 0   :: non_neg_integer(),
            match_index = 0  :: non_neg_integer()
           }).

-type raft_node() :: #raft_node{}.

-record(metadata, {
            name             :: atom(),
            nodes = []       :: [raft_node()],
            term = 0         :: non_neg_integer(),
            votes = []       :: [atom()],
            voted_for = null :: atom(),
            leader_id = null :: atom()
           }).

-type metadata() :: #metadata{}.


-record(client_message, {}).
