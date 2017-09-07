-record(raft_node, {
            name             :: atom(),
            next_index = 0   :: non_neg_integer(),
            match_index = 0  :: non_neg_integer()
           }).

-type raft_node() :: #raft_node{}.


-record(log_entry, {index, term, command}).

-type log_entry() :: #log_entry{}.


-record(client_message, {
            client_id    :: atom(),
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
