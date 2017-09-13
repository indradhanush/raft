-module(raft_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../src/raft.hrl").

%%%===================================================================
%% Tests for internal functions
%%%===================================================================

assert_options([{timeout, Timeout, ticker}]) ->
    [?_assert(Timeout >= ?TIMEOUT_SEED),
     ?_assert(Timeout =< ?TIMEOUT_SEED + ?TIMEOUT_RANGE)].

test_get_timeout_options_arity_0() ->
    assert_options([raft:get_timeout_options()]).


get_timeout_options_test_() ->
    [test_get_timeout_options_arity_0(),
     ?_assertEqual(raft:get_timeout_options(10), {timeout, 10, ticker})].


%%%===================================================================
%% Tests for state machine callbacks
%%%===================================================================

test_init_types() ->
    Result = raft:init([n1]),
    {_, _, _, Options} = Result,

    Expected = {
        ok,
        follower,
        raft:create_metadata(
            n1, [#raft_node{name = n2},
                 #raft_node{name = n3},
                 #raft_node{name = n4},
                 #raft_node{name = n5}]),
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].


init_test_() ->
    [test_init_types()].


initial_metadata() ->
    Nodes = lists:delete(#raft_node{name = n1}, ?NODES),
    raft:create_metadata(n1, Nodes).

follower_setup() ->
    Metadata = initial_metadata(),
    Metadata#metadata{term = 5}.

test_follower_timeout(#metadata{term = Term} = Metadata) ->
    Result = raft:follower(timeout, ticker, Metadata#metadata{voted_for = n2}),

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        candidate,
        ExpectedMetadata#metadata{term = Term},
        [{timeout, 0, ticker}]
       },

    [?_assertEqual(Expected, Result)].

test_follower_vote_request_not_voted_yet(#metadata{term = Term} = Metadata) ->
    Result = raft:follower(cast,
                           #vote_request{term = Term+1, candidate_id = n2},
                           Metadata),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term+1, voted_for = n2},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_vote_request_with_candidate_older_term(
  #metadata{term = Term} = Metadata) ->

    Result = raft:follower(cast,
                           #vote_request{term = Term-1, candidate_id = n2},
                           Metadata#metadata{voted_for = n3}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term, voted_for = n3},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_vote_request_with_already_voted(#metadata{term = Term} = Metadata) ->
    Result = raft:follower(cast,
                           #vote_request{term = Term, candidate_id = n2},
                           Metadata#metadata{voted_for = n3}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term, voted_for = n3},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_vote_request_with_new_term_but_already_voted(
  #metadata{term = Term} = Metadata) ->

    Result = raft:follower(cast,
                           #vote_request{term = Term+1, candidate_id = n2},
                           Metadata#metadata{voted_for = n3}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term+1, voted_for = n2},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_vote_granted(#metadata{} = Metadata) ->
    Result = raft:follower(cast, #vote_granted{}, Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_heartbeat_just_after_voting(#metadata{term = Term} = Metadata) ->
    Result = raft:follower(cast,
                           #append_entries{term = Term, leader_id = n2},
                           Metadata#metadata{voted_for = n2}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{
            term = Term,
            votes = [],
            voted_for = n2,
            leader_id = n2
           },
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_heartbeat_with_older_term(#metadata{term = Term} = Metadata) ->
    Result = raft:follower(cast,
                           #append_entries{term = Term-1, leader_id = n2},
                           Metadata#metadata{voted_for = n3, leader_id = n3}),
    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        candidate,
        ExpectedMetadata#metadata{
            term = Term,
            votes = [],
            voted_for = n3,
            leader_id = n3
           },
        [{timeout, 0, ticker}]
       },

    [?_assertEqual(Expected, Result)].

test_follower_client_message(#metadata{} = Metadata) ->
    Result = raft:follower(cast,
                           #client_message{client_id = self()},
                           Metadata#metadata{leader_id = n2}),

    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].


follower_test_() ->
    [
     {
         "Follower promotes itself to candidate if times out",
         {setup, fun follower_setup/0, fun test_follower_timeout/1}
     },
     {
         "Follower received a vote request but has not voted in yet"
         " and votes in favour",
         {setup, fun follower_setup/0, fun test_follower_vote_request_not_voted_yet/1}
     },
     {
         "Follower received a vote request but candidate has an older term",
         {
             setup,
             fun follower_setup/0,
             fun test_follower_vote_request_with_candidate_older_term/1
         }
     },
     {
         "Follower received a vote request but follower has already voted for"
         " this term",
         {
             setup,
             fun follower_setup/0,
             fun test_follower_vote_request_with_already_voted/1
         }
     },
     {
         "Follower received a vote request but follower has already voted"
         " in the previous term, however, this is a new term and it votes again"
         " in favour of the new candidate",
         {
             setup,
             fun follower_setup/0,
             fun test_follower_vote_request_with_new_term_but_already_voted/1
         }
     },
     {
         "Follower received a vote granted but ignores it",
         {setup, fun follower_setup/0, fun test_follower_vote_granted/1}
     },
     {
         "Follower received a heartbeat after sending a vote",
         {setup, fun follower_setup/0, fun test_follower_heartbeat_just_after_voting/1}
     },
     {
         "Follower received a heartbeat but from with an older term and promotes"
         " itself to candidate",
         {setup, fun follower_setup/0, fun test_follower_heartbeat_with_older_term/1}
     },
     {
         "Follower received a client message and sends a cast back to the client",
         {setup, fun follower_setup/0, fun test_follower_client_message/1}
     }
    ].


candidate_setup() ->
    Metadata = initial_metadata(),
    Metadata#metadata{term = 6, votes = [n1], voted_for = n1}.

test_candidate_timeout(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(timeout, ticker, Metadata),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term+1, votes = [n1], voted_for = n1},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_request_with_older_term(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(cast,
                            #vote_request{term = Term-1, candidate_id = n2},
                            Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_request_with_newer_term(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(cast,
                            #vote_request{term = Term+1, candidate_id = n2},
                            Metadata),
    {_, _, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{term = Term+1, votes = [], voted_for = n2},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_granted_but_no_majority(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(cast,
                            #vote_granted{term = Term, voter_id = n2},
                            Metadata),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term, votes = [n1, n2], voted_for = n1},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_granted_with_majority(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(cast,
                            #vote_granted{term = Term, voter_id = n3},
                            Metadata#metadata{votes = [n1, n2]}),
    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state, leader,
        ExpectedMetadata#metadata{term = Term, votes = [n1, n2, n3], voted_for = n1},
        [{timeout, 0, ticker}]
       },

    [?_assertEqual(Expected, Result)].

test_candidate_vote_granted_but_older_term(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(cast,
                            #vote_granted{term = Term-1, voter_id = n2},
                            Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_heartbeat_with_older_term(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(cast,
                            #append_entries{term = Term-1, leader_id = n2},
                            Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_heartbeat_with_newer_term(#metadata{term = Term} = Metadata) ->
    Result = raft:candidate(cast,
                            #append_entries{term = Term+1, leader_id = n2},
                            Metadata),
    {_, _, _,  Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{
            term = Term+1,
            votes = [],
            voted_for = null,
            leader_id = n2
           },
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_client_message(#metadata{} = Metadata) ->
    Result = raft:candidate(cast,
                            #client_message{client_id = self()},
                            Metadata#metadata{leader_id = n2}),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].


candidate_test_() ->
    [
     {
         "Candidate times out and starts a new election",
         {setup, fun candidate_setup/0, fun test_candidate_timeout/1}
     },
     {
         "Candidate receives a vote request but with an older term",
         {
             setup,
             fun candidate_setup/0,
             fun test_candidate_vote_request_with_older_term/1
         }
     },
     {
         "Candidate receives a vote request with a newer term, votes in favour"
         " and steps down to follower",
         {
             setup,
             fun candidate_setup/0,
             fun test_candidate_vote_request_with_newer_term/1
         }
     },
     {
         "Candidate receives a heartbeat with an older term",
         {
             setup,
             fun candidate_setup/0,
             fun test_candidate_heartbeat_with_older_term/1
         }
     },
     {
         "Candidate receives a heartbeat with a newer term, steps down to follower",
         {
             setup,
             fun candidate_setup/0,
             fun test_candidate_heartbeat_with_newer_term/1
         }
     },
     {
         "Candidate receives a vote but has not attained majority and remains a"
         " candidate",
         {
             setup,
             fun candidate_setup/0,
             fun test_candidate_vote_granted_but_no_majority/1
         }
     },
     {
         "Candidate receives a vote and has attained majority and becomes a leader",
         {setup, fun candidate_setup/0, fun test_candidate_vote_granted_with_majority/1}
     },
     {
         "Candidate receives a vote and the term is outdated",
         {
             setup,
             fun candidate_setup/0,
             fun test_candidate_vote_granted_but_older_term/1
         }
     },
     {
         "Candidate received a client message and sends a cast back to the client",
         {setup, fun candidate_setup/0, fun test_candidate_client_message/1}
     }
    ].


leader_setup() ->
    Metadata = initial_metadata(),
    Metadata#metadata{term = 7, votes = [n1, n2], voted_for = n1}.

leader_options() ->
    [{timeout, ?HEARTBEAT_TIMEOUT, ticker}].

test_leader_timeout_with_empty_log(#metadata{term = Term} = Metadata) ->
    Result = raft:leader(timeout, ticker, Metadata),

    InitialMetadata = initial_metadata(),
    UpdatedNodes = [
        Node#raft_node{next_index = 1} || Node <- InitialMetadata#metadata.nodes
    ],
    ExpectedMetadata = InitialMetadata#metadata{
                           term = Term,
                           nodes = UpdatedNodes,
                           votes = [n1, n2],
                           voted_for = n1
                          },

    Expected = {keep_state, ExpectedMetadata, leader_options()},

    [?_assertEqual(Expected, Result)].

test_leader_vote_request_with_older_term(#metadata{term = Term} = Metadata) ->
    Result = raft:leader(
                 cast,
                 #vote_request{term = Term-1, candidate_id = n2},
                 Metadata),

    Expected = {keep_state_and_data, leader_options()},

    [?_assertEqual(Expected, Result)].

test_leader_vote_request_with_newer_term(#metadata{term = Term} = Metadata) ->
    Result = raft:leader(cast,
                         #vote_request{term = Term+1, candidate_id = n2},
                         Metadata),

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{term = Term+1, votes = [], voted_for = n2},
        leader_options()
       },

    [?_assertEqual(Expected, Result)].

test_leader_vote_granted(#metadata{term = Term} = Metadata) ->
    Result = raft:leader(cast, #vote_granted{term = Term, voter_id = n2}, Metadata),

    Expected = {keep_state_and_data, leader_options()},

    [?_assertEqual(Expected, Result)].

test_leader_heartbeat_with_older_term(#metadata{term = Term} = Metadata) ->
    Result = raft:leader(cast,
                    #append_entries{term = Term-1, leader_id = n2, entries = []},
                    Metadata),

    Expected = {keep_state_and_data, leader_options()},

    [?_assertEqual(Expected, Result)].

test_leader_heartbeat_with_newer_term(#metadata{term = Term} = Metadata) ->
    Result = raft:leader(cast,
                    #append_entries{term = Term+1, leader_id = n2, entries = []},
                    Metadata),

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{
            term = Term+1,
            votes = [],
            voted_for = null,
            leader_id = n2
           },
        leader_options()
       },

    [?_assertEqual(Expected, Result)].

test_leader_client_message(#metadata{term = Term} = Metadata) ->
    ClientMessage = #client_message{
                         client_id = self(),
                         message_id = "unique-message-id",
                         command = "test"
                        },

    Result = raft:leader(cast, ClientMessage, Metadata),

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{
            term = Term,
            votes = [n1, n2],
            voted_for = n1,
            log = [#log_entry{
                      index = 1,
                      term = Term,
                      command = "test"
                     }],
            to_reply = [ClientMessage]
           },
        leader_options()
       },

    [?_assertEqual(Expected, Result)].


leader_test_() ->
    [
     {
         "Leader times out and sends heartbeats with an empty log",
         {setup, fun leader_setup/0, fun test_leader_timeout_with_empty_log/1}
     },
     {
         "Leader receives a vote request but with an older term"
         " and ignores the request",
         {setup, fun leader_setup/0, fun test_leader_vote_request_with_older_term/1}
     },
     {
         "Leader receives a vote request but with the newer term"
         " and steps down to follower",
         {setup, fun leader_setup/0, fun test_leader_vote_request_with_newer_term/1}
     },
     {
         "Leader receives a vote granted but ignores it",
         {setup, fun leader_setup/0, fun test_leader_vote_granted/1}
     },
     {
         "Leader receives a heartbeat but with an older term and ignores it",
         {setup, fun leader_setup/0, fun test_leader_heartbeat_with_older_term/1}
     },
     {
         "Leader receives a heartbeat but with a newer term and steps down to follower",
         {setup, fun leader_setup/0, fun test_leader_heartbeat_with_newer_term/1}
     },
     {
         "Leader receives a client message and sends append entries RPCs"
         " and updates its log",
         {setup, fun leader_setup/0, fun test_leader_client_message/1}
     }
    ].

