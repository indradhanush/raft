-module(raft_client_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../src/raft.hrl").


%%%===================================================================
%%% Tests
%%%===================================================================


setup() ->
    raft:start_link(n1),
    raft:start_link(n2),
    raft:start_link(n3),

    [n1, n2, n3].


candidate_setup() ->
    raft:start_link(n1),
    [n1].


teardown(Nodes) ->
    [raft:stop(Node) || Node <- Nodes].


create_client_message(MessageId, Command) ->
    #client_message{
         client_id = self(),
         message_id = MessageId,
         command = Command
        }.


assert_follower_state(Node, TestName) ->
    {State, #metadata{
                 name = Name,
                 nodes = Nodes,
                 term = Term,
                 votes = Votes,
                 voted_for = VotedFor,
                 leader_id = LeaderId
                }
    } = sys:get_state(Node),

    ExpectedNodes = lists:delete(#raft_node{name = Node}, ?NODES),

    [
     ?_assertEqual(follower, State),
     ?_assertEqual(Node, Name),
     ?_assertEqual(ExpectedNodes, Nodes),
     ?_assertEqual(1, Term),
     ?_assertEqual([], Votes),
     ?_assertEqual({TestName, n1}, {TestName, VotedFor}),
     ?_assertEqual({TestName, n1}, {TestName, LeaderId})
    ].

assert_candidate_state_with_no_leader(Node, TestName) ->
    {State, #metadata{
                 name = Name,
                 nodes = Nodes,
                 term = Term,
                 votes = Votes,
                 voted_for = VotedFor,
                 leader_id = LeaderId
                }
    } = sys:get_state(Node),

    ExpectedNodes = lists:delete(#raft_node{name = Node}, ?NODES),

    [
     ?_assertEqual(candidate, State),
     ?_assertEqual({TestName, Node}, {TestName, Name}),
     ?_assertEqual({TestName, ExpectedNodes}, {TestName, Nodes}),
     ?_assertEqual({TestName, 1}, {TestName, Term}),
     ?_assertEqual({TestName, [n1]}, {TestName, Votes}),
     ?_assertEqual({TestName, n1}, {TestName, VotedFor}),
     ?_assertEqual({TestName, null}, {TestName, LeaderId})
    ].

assert_candidate_state_with_leader(Node, TestName) ->
    {State, #metadata{
                 name = Name,
                 nodes = Nodes,
                 term = Term,
                 votes = Votes,
                 voted_for = VotedFor,
                 leader_id = LeaderId
                }
    } = sys:get_state(Node),

    ExpectedNodes = [
        X#raft_node{next_index = 1} || X <- lists:delete(#raft_node{name = Node}, ?NODES)
    ],

    [
     ?_assertEqual(leader, State),
     ?_assertEqual({TestName, Node}, {TestName, Name}),
     ?_assertEqual({TestName, ExpectedNodes}, {TestName, Nodes}),
     ?_assertEqual({TestName, 2}, {TestName, Term}),
     ?_assertEqual({TestName, [n1, n2, n3]}, {TestName, lists:sort(Votes)}),
     ?_assertEqual({TestName, Node}, {TestName, VotedFor}),
     ?_assertEqual({TestName, n1}, {TestName, LeaderId})
    ].


assert_leader_state(Node) ->
    {State, #metadata{votes = Votes}} = sys:get_state(Node),
    [?_assertEqual(leader, State),
     ?_assertEqual([n1, n2, n3], lists:sort(Votes))].


test_write_to_follower_with_no_leader(_Nodes) ->
    {State, _} = sys:get_state(n1),

    Response = raft_client:write(n1,
                     create_client_message(test_message_id, "test command")),
    [
     ?_assertEqual(follower, State),
     ?_assertEqual({error, null}, Response)
    ].


test_write_to_follower_with_leader(_Nodes) ->
    gen_statem:cast(n1, test_timeout),

    %% The sleep is enough for the nodes to send and receive the
    %% message and complete a leader election
    timer:sleep(1),

    Response = raft_client:write(
                   n2,
                   create_client_message(test_message_id, "test command")
                  ),

    [assert_follower_state(n2, test_write_to_follower_with_leader),
     assert_follower_state(n3, test_write_to_follower_with_leader),
     assert_leader_state(n1),
     ?_assertEqual({error, n1}, Response)].


test_write_to_candidate_with_no_leader(_Nodes) ->
    gen_statem:cast(n1, test_timeout),

    Response = raft_client:write(
                   n1,
                   create_client_message(test_message_id, "test command")
                  ),

    [assert_candidate_state_with_no_leader(n1, test_write_to_candidate_with_no_leader),
     ?_assertEqual({error, null}, Response)].


test_write_to_candidate_with_leader(_Nodes) ->
    gen_statem:cast(n1, test_timeout),

    %% The sleep is enough for the nodes to send and receive the
    %% message and complete a leader election
    timer:sleep(1),

    gen_statem:cast(n2, test_timeout),

    Response = raft_client:write(
                   n2,
                   create_client_message(test_message_id, "test command")
                  ),

    [assert_candidate_state_with_leader(n2, test_write_to_candidate_with_leader),
     ?_assertEqual({error, n1}, Response)].


test_write_to_leader(_Nodes) ->
    gen_statem:cast(n1, test_timeout),

    %% The sleep is enough for the nodes to send and receive the
    %% message and complete a leader election
    timer:sleep(1),

    Response = raft_client:write(
                   n1,
                   create_client_message(test_message_id, "test command")
                  ),

    [assert_follower_state(n2, test_write_to_leader),
     assert_follower_state(n3, test_write_to_leader),
     assert_leader_state(n1),
     ?_assertEqual(ok, Response)].


client_test_() ->
    [
     {
         "Client tries to write to a follower with no leader elected",
         {
             setup,
             fun setup/0,
             fun teardown/1,
             fun test_write_to_follower_with_no_leader/1
         }
     },
     {
         "Client tries to write to a follower with leader elected",
         {
             setup,
             fun setup/0,
             fun teardown/1,
             fun test_write_to_follower_with_leader/1
         }
     },
     {
         "Client tries to write to a candidate with no leader elected",
         {
             setup,
             fun candidate_setup/0,
             fun teardown/1,
             fun test_write_to_candidate_with_no_leader/1
         }
     },
     {
         "Client tries to write to a candidate with a leader elected",
         {
             setup,
             fun setup/0,
             fun teardown/1,
             fun test_write_to_candidate_with_leader/1
         }
     },
     {
         "Client tries to write to a leader",
         {setup, fun setup/0, fun teardown/1, fun test_write_to_leader/1}
     }
    ].
