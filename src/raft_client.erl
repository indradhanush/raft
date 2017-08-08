-module(raft_client).

-include("raft.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([write/2]).


write(Node, #client_message{}=Message) ->
    case gen_statem:call(Node, Message) of
        {error, null} ->
            {error, null};
        {error, LeaderId} ->
            write(LeaderId, Message);
        {ok, awesome} ->
            {ok, awesome}
    end.


%%%===================================================================
%%% Tests
%%%===================================================================


setup() ->
    raft:start_link(n1),
    raft:start_link(n2),
    raft:start_link(n3),
    [n1, n2, n3].


teardown(Nodes) ->
    [raft:stop(Node) || Node <- Nodes].

setup_candidate() ->
    raft:start_link(n1),
    [n1].


assert_follower_state(Node) ->
    {State, #metadata{term=Term, voted_for=VotedFor}} = sys:get_state(Node),

    [?_assertEqual(follower, State),
     ?_assertEqual({Node, n1}, {Node, VotedFor}),
     ?_assertEqual(1, Term)].

assert_candidate_state(Node) ->
    {State, #metadata{votes=Votes}} = sys:get_state(Node),
    [?_assertEqual(candidate, State),
     ?_assertEqual([n1], Votes)].

assert_leader_state(Node) ->
    {State, #metadata{votes=Votes}} = sys:get_state(Node),
    [?_assertEqual(leader, State),
     ?_assertEqual([n1, n2, n3], lists:sort(Votes))].


test_write_to_follower_with_no_leader(_Nodes) ->
    {follower, _} = sys:get_state(n1),
    ?_assertEqual({error, null}, write(n1, #client_message{})).

test_write_to_candidate(_Nodes) ->
    gen_statem:cast(n1, test_timeout),

    %% The sleep is enough for the nodes to send and receive the
    %% message and complete a leader election, but we have only one
    %% node running here and as a result it remains locked in the
    %% candidate state.
    timer:sleep(1),

    [assert_candidate_state(n1),
     ?_assertEqual({error, null}, write(n1, #client_message{}))].

test_write_to_leader(_Nodes) ->
    gen_statem:cast(n1, test_timeout),

    %% The sleep is enough for the nodes to send and receive the
    %% message and complete a leader election
    timer:sleep(1),

    [assert_follower_state(n2),
     assert_follower_state(n3),
     assert_leader_state(n1),
     ?_assertEqual({ok, awesome}, write(n1, #client_message{}))].


client_test_() ->
    [
     {
         "Client tries to write to a follower with no leader elected",
         {setup, fun setup/0, fun teardown/1, fun test_write_to_follower_with_no_leader/1}
     },
     {
         "Client tries to write to a candidate with no leader elected",
         {setup, fun setup_candidate/0, fun teardown/1, fun test_write_to_candidate/1}
     },
     {
         "Client tries to write to a leader",
         {setup, fun setup/0, fun teardown/1, fun test_write_to_leader/1}
     }
    ].
