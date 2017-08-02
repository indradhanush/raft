-module(raft).

-behaviour(gen_statem).

%% API
-export([start/0, start_link/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([follower/3, candidate/3, leader/3]).

-define(SERVER, ?MODULE).
-define(HEARTBEAT_TIMEOUT, 20).

-ifdef(TEST).
-define(TIMEOUT_SEED, 3000).
-else.
-define(TIMEOUT_SEED, 150).
-endif.


-record(metadata, {name, nodes, term, votes = [], voted_for = null}).

-record(vote_request, {term, candidate_id}).

-record(vote_granted, {term, voter_id}).

-record(append_entries, {term, leader_id, entries = []}).


%%%===================================================================
%%% Public API
%%%===================================================================
start() ->
    raft:start_link(n1),
    raft:start_link(n2),
    raft:start_link(n3),
    raft:start_link(n4),
    raft:start_link(n5).

start_link(Name) ->
    gen_statem:start_link({local, Name}, ?MODULE, [Name], []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> state_functions.

-spec init(Args :: term()) ->
                  gen_statem:init_result(atom()).
init([Name]) ->
    {ok,
     follower,
     #metadata{name=Name, nodes=lists:delete(Name, [n1, n2, n3]), term=0},
     [get_timeout_options()]}.

%% -spec state_name('enter',
%%                  OldState :: atom(),
%%                  Data :: term()) ->
%%                         gen_statem:state_enter_result('state_name');
%%                 (gen_statem:event_type(),
%%                  Msg :: term(),
%%                  Data :: term()) ->
%%                         gen_statem:event_handler_result(atom()).
%% state_name({call,Caller}, _Msg, Data) ->
%%     {next_state, state_name, Data, [{reply,Caller,ok}]}.

follower(timeout, ticker, #metadata{name=Name}=Data) when is_atom(Name) ->
    %% Start an election
    log("timeout", Data, []),
    {next_state, candidate, Data#metadata{votes=[], voted_for=null}, [get_timeout_options(0)]};

follower(cast, #vote_request{candidate_id=CandidateId}=VoteRequest, #metadata{name=Name, voted_for=null}=Data) ->
    log("Received vote request from: ~p", Data, [CandidateId]),
    VotedFor = case can_grant_vote(VoteRequest, Data) of
                   true ->
                       send_vote(Name, VoteRequest),
                       log("Vote sent to ~p", Data, [CandidateId]),
                       CandidateId;
                   false ->
                       log("Vote denied to ~p", Data, [CandidateId]),
                       null
               end,
    {keep_state, with_latest_term(VoteRequest, Data#metadata{voted_for=VotedFor}), [get_timeout_options()]};

follower(cast, #vote_request{candidate_id=CandidateId}=VoteRequest, #metadata{}=Data) ->
    log("Received vote request from: ~p, but already voted", Data, [CandidateId]),
    {keep_state, with_latest_term(VoteRequest, Data), [get_timeout_options()]};

follower(cast, #vote_granted{}, #metadata{}=Data) ->
    log("Received vote in follower state", Data, []),
    {keep_state_and_data, [get_timeout_options()]};

follower(cast,
         #append_entries{term=Term, leader_id=LeaderId, entries=[]},
         #metadata{term=CurrentTerm}=Data) ->
    %% TODO: This case is not required. Currently added for logging.
    case is_valid_term(Term, CurrentTerm) of
        true ->
            log("Received heartbeat from ~p", Data, [LeaderId]);
        false ->
            log("Received heartbeat from ~p but it has outdated term", Data, [LeaderId])
    end,
    {keep_state_and_data, [get_timeout_options()]}.

candidate(timeout, ticker, #metadata{name=Name, term=Term}=Data) ->
    UpdatedData = Data#metadata{term=Term+1, votes=[Name], voted_for=Name},
    log("starting election", Data, []),
    start_election(UpdatedData),
    {keep_state, UpdatedData, [get_timeout_options()]};

candidate(cast,
          #vote_request{candidate_id=CandidateId}=VoteRequest,
          #metadata{name=Name}=Data) ->
    log("Received vote request in candidate state", Data, []),

    case can_grant_vote(VoteRequest, Data) of
        true ->
            log("Candidate stepping down. Received vote request for a new term from ~p", Data, [CandidateId]),
            send_vote(Name, VoteRequest),
            {
              next_state,
              follower,
              with_latest_term(VoteRequest, Data#metadata{votes=[], voted_for=CandidateId}),
              [get_timeout_options()]
            };
        false ->
            {keep_state_and_data, [get_timeout_options()]}
    end;

candidate(cast,
          #vote_granted{term=Term, voter_id=Voter},
          #metadata{nodes=Nodes, term=CurrentTerm, votes=Votes}=Data) ->

    case is_valid_term(Term, CurrentTerm) of
        false ->
            {keep_state_and_data, [get_timeout_options()]};
        true ->
            UpdatedVotes = lists:append(Votes, [Voter]),
            log("Current votes for candidate ~p", Data, [UpdatedVotes]),
            case has_majority(UpdatedVotes, Nodes) of
                true ->
                    log("Elected as Leader", Data, []),
                    {next_state, leader, Data#metadata{votes=UpdatedVotes}, [get_timeout_options(0)]};
                false ->
                    {keep_state, Data#metadata{votes=UpdatedVotes}, [get_timeout_options()]}
            end
    end;

candidate(cast,
          #append_entries{term=Term, leader_id=LeaderId, entries=[]},
          #metadata{term=CurrentTerm}=Data) ->
    case is_valid_term(Term, CurrentTerm) of
        true ->
            log("Received heartbeat from ~p in candidate state with new term. Stepping down", Data, [LeaderId]),
            {next_state, follower, Data#metadata{term=Term, votes=[], voted_for=null}, [get_timeout_options()]};
        false ->
            {keep_state_and_data, [get_timeout_options()]}
    end.


leader(timeout, ticker, #metadata{term=Term, name=Name, nodes=Nodes}=Data) ->
    log("Leader timeout, sending Heartbeat", Data, []),
    Heartbeat = #append_entries{term=Term, leader_id=Name},
    [send_heartbeat(Node, Heartbeat) || Node <- Nodes],
    {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]};

leader(cast,
       #vote_request{candidate_id=CandidateId}=VoteRequest,
       #metadata{name=Name}=Data) ->
    log("Received vote request in leader state", Data, []),
    case can_grant_vote(VoteRequest, Data) of
        true ->
            log("Stepped down and voted", Data, []),
            send_vote(Name, VoteRequest),
            {
              next_state,
              follower,
              with_latest_term(VoteRequest, Data#metadata{votes=[], voted_for=CandidateId}),
              [get_timeout_options(?HEARTBEAT_TIMEOUT)]
            };
        false ->
            {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]}
    end;

leader(cast, #vote_granted{}, #metadata{}=Data) ->
    log("Received vote granted in leader state", Data, []),
    {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]};

leader(cast, #append_entries{term=Term, entries=[]}, #metadata{term=CurrentTerm}=Data) ->
    case is_valid_term(Term, CurrentTerm) of
        true ->
            {next_state, follower, Data#metadata{term=Term, votes=[], voted_for=null}, [get_timeout_options(?HEARTBEAT_TIMEOUT)]};
        false ->
            {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]}
    end.


-spec terminate(Reason :: term(), State :: term(), Data :: term()) ->
                       any().
terminate(_Reason, _State, Data) ->
    {next_state, eof, Data, [get_timeout_options()]}.


-spec code_change(
        OldVsn :: term() | {down,term()},
        State :: term(), Data :: term(), Extra :: term()) ->
                         {ok, NewState :: term(), NewData :: term()} |
                         (Reason :: term()).
code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_timeout_options() ->
    {timeout, Timeout, ticker} = get_timeout_options(rand:uniform(150)),
    {timeout, ?TIMEOUT_SEED + Timeout, ticker}.

get_timeout_options(Time) ->
    {timeout, Time, ticker}.

log(Message, #metadata{name=Name, term=Term}, Args) ->
    FormattedMessage = io_lib:format(Message, Args),
    io:format("[~p Term #~p]: ~s~n", [Name, Term, FormattedMessage]).

has_majority(Votes, Nodes) when is_list(Votes), is_list(Nodes) ->
    length(Votes) >= (length(Nodes) div 2) + 1.

start_election(#metadata{name=Name, nodes=Nodes, term=Term}) ->
    VoteRequest = #vote_request{term=Term, candidate_id=Name},
    [request_vote(Voter, VoteRequest) || Voter <- Nodes].


can_grant_vote(#vote_request{term=CandidateTerm}, #metadata{term=CurrentTerm})
  when is_integer(CandidateTerm), is_integer(CurrentTerm) ->
    CandidateTerm >= CurrentTerm.

is_valid_term(Term, CurrentTerm) ->
    Term >= CurrentTerm.

with_latest_term(#vote_request{term=CandidateTerm}, #metadata{term=CurrentTerm}=Data) ->
    if CandidateTerm >= CurrentTerm ->
            Data#metadata{term=CandidateTerm};
       CandidateTerm < CurrentTerm ->
            Data
    end.

request_vote(Voter, VoteRequest) ->
    gen_statem:cast(Voter, VoteRequest).


send_vote(Name, #vote_request{term=Term, candidate_id=CandidateId}) ->
    VoteGranted = #vote_granted{term=Term, voter_id=Name},
    gen_statem:cast(CandidateId, VoteGranted).


send_heartbeat(Node, Heartbeat) ->
    gen_statem:cast(Node, Heartbeat).


%%%===================================================================
%% Tests for internal functions
%%%===================================================================

%% -ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


assert_options([{timeout, Timeout, ticker}]) ->
    [?_assert(Timeout >= ?TIMEOUT_SEED),
    ?_assert(Timeout < ?TIMEOUT_SEED+150)].

test_get_timeout_options_arity_0() ->
    assert_options([get_timeout_options()]).


get_timeout_options_test_() ->
    [test_get_timeout_options_arity_0(),
     ?_assertEqual(get_timeout_options(10), {timeout, 10, ticker})].


%%%===================================================================
%% Tests for state machine callbacks
%%%===================================================================

test_init_types() ->
    Result = init([test]),
    {_, _, _, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {ok, follower, #metadata{name=test, nodes=[n1, n2, n3], term=0, votes=[], voted_for=null}, Options},
        Result
       )
    ].


init_test_() ->
    [test_init_types()].


follower_setup() ->
    #metadata{name=n1, nodes=[n2, n3], term=5}.

test_follower_timeout(#metadata{term=Term}=Metadata) ->
    Result = follower(timeout, ticker, Metadata#metadata{voted_for=n2}),

    [
     ?_assertEqual(
        {
          next_state,
          candidate,
          #metadata{name=n1, nodes=[n2, n3], term=Term, votes=[], voted_for=null},
          [{timeout, 0, ticker}]
        },
        Result
       )
    ].

test_follower_vote_request(#metadata{term=Term}=Metadata) ->
    Result = follower(cast, #vote_request{term=Term+1, candidate_id=n2}, Metadata#metadata{}),
    {_, _, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state, #metadata{name=n1, nodes=[n2, n3], term=Term+1, votes=[], voted_for=n2}, Options},
        Result
       )
     ].

test_follower_vote_request_with_candidate_older_term(#metadata{term=Term}=Metadata) ->
    Result = follower(cast, #vote_request{term=Term-1, candidate_id=n2}, Metadata),
    {_, _, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state, #metadata{name=n1, nodes=[n2, n3], term=Term, voted_for=null}, Options},
        Result
       )
    ].

test_follower_vote_request_with_already_voted(#metadata{term=Term}=Metadata) ->
    Result = follower(cast, #vote_request{term=Term, candidate_id=n2}, Metadata#metadata{voted_for=n3}),
    {_, _, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state, #metadata{name=n1, nodes=[n2, n3], term=Term, voted_for=n3}, Options},
        Result
       )
    ].

test_follower_vote_granted(#metadata{}=Metadata) ->
    Result = follower(cast, #vote_granted{}, Metadata),
    {_, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state_and_data, Options},
        Result
       )
    ].

test_follower_heartbeat_just_after_voting(#metadata{term=Term}=Metadata) ->
    Result = follower(cast, #append_entries{term=Term+1, leader_id=n2}, Metadata#metadata{voted_for=n2}),
    {_, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state_and_data, Options},
        Result
       )
    ].


follower_test_() ->
    [
     {
       "Follower promotes itself to candidate if times out",
       {setup, fun follower_setup/0, fun test_follower_timeout/1}
     },
     {
       "Follower received a vote request and votes in favour",
       {setup, fun follower_setup/0, fun test_follower_vote_request/1}
     },
     {
       "Follower received a vote request but candidate has an older term",
       {setup, fun follower_setup/0, fun test_follower_vote_request_with_candidate_older_term/1}
     },
     {
       "Follower received a vote request but follower has already voted",
       {setup, fun follower_setup/0, fun test_follower_vote_request_with_already_voted/1}
     },
     {
       "Follower received a vote granted but ignores it",
       {setup, fun follower_setup/0, fun test_follower_vote_granted/1}
     },
     {
       "Follower received a heartbeat after sending a vote",
       {setup, fun follower_setup/0, fun test_follower_heartbeat_just_after_voting/1}
     }
    ].


candidate_setup() ->
    #metadata{name=n1, nodes=[n2, n3], term=6, votes=[n1], voted_for=n1}.

test_candidate_timeout(#metadata{term=Term}=Metadata) ->
    Result = candidate(timeout, ticker, Metadata),
    {_, _, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state, #metadata{name=n1, nodes=[n2, n3], term=Term+1, votes=[n1], voted_for=n1}, Options},
        Result
       )
    ].

test_candidate_vote_request_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #vote_request{term=Term-1, candidate_id=n2}, Metadata),
    {_, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state_and_data, Options},
        Result
       )
    ].

test_candidate_vote_request_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #vote_request{term=Term+1, candidate_id=n2}, Metadata),
    {_, _, _, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {next_state, follower, #metadata{name=n1, nodes=[n2, n3], term=Term+1, votes=[], voted_for=n2}, Options},
        Result
       )
    ].

test_candidate_vote_granted_but_no_majority(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #vote_granted{term=Term, voter_id=n2}, Metadata#metadata{nodes=[n2, n3, n4, n5]}),
    {_, _, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state, #metadata{name=n1, nodes=[n2, n3, n4, n5], term=Term, votes=[n1, n2], voted_for=n1}, Options},
        Result
       )
    ].

test_candidate_vote_granted_with_majority(#metadata{term=Term}=Metadata) ->
    Result = candidate(
               cast,
               #vote_granted{term=Term, voter_id=n3},
               Metadata#metadata{nodes=[n2, n3, n4, n5], votes=[n1, n2]}
              ),

    [
     ?_assertEqual(
        {
          next_state, leader,
          #metadata{name=n1, nodes=[n2, n3, n4, n5], term=Term, votes=[n1, n2, n3], voted_for=n1},
          [{timeout, 0, ticker}]
        },
        Result
       )
    ].

test_candidate_vote_granted_but_older_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #vote_granted{term=Term-1, voter_id=n2}, Metadata),
    {_, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state_and_data, Options},
        Result
       )
    ].

test_candidate_heartbeat_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #append_entries{term=Term-1, leader_id=n2}, Metadata),
    {_, Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {keep_state_and_data, Options},
        Result
       )
    ].

test_candidate_heartbeat_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #append_entries{term=Term+1, leader_id=n2}, Metadata),
    {_, _, _,  Options} = Result,

    [
     assert_options(Options),
     ?_assertEqual(
        {next_state, follower, #metadata{name=n1, nodes=[n2, n3], term=Term+1, votes=[], voted_for=null}, Options},
        Result
       )
    ].



candidate_test_() ->
    [
     {
       "Candidate times out and starts a new election",
       {setup, fun candidate_setup/0, fun test_candidate_timeout/1}
     },
     {
       "Candidate receives a vote request but with an older term",
       {setup, fun candidate_setup/0, fun test_candidate_vote_request_with_older_term/1}
     },
     {
       "Candidate receives a vote request with a newer term, votes in favour and steps down to follower",
       {setup, fun candidate_setup/0, fun test_candidate_vote_request_with_newer_term/1}
     },
     {
       "Candidate receives a heartbeat with an older term",
       {setup, fun candidate_setup/0, fun test_candidate_heartbeat_with_older_term/1}
     },
     {
       "Candidate receives a heartbeat with a newer term, steps down to follower",
       {setup, fun candidate_setup/0, fun test_candidate_heartbeat_with_newer_term/1}
     },
     {
       "Candidate receives a vote but has not attained majority and remains a candidate",
       {setup, fun candidate_setup/0, fun test_candidate_vote_granted_but_no_majority/1}
     },
     {
       "Candidate receives a vote and has attained majority and becomes a leader",
       {setup, fun candidate_setup/0, fun test_candidate_vote_granted_with_majority/1}
     },
     {
       "Candidate receives a vote and the term is outdated",
       {setup, fun candidate_setup/0, fun test_candidate_vote_granted_but_older_term/1}
     }
    ].

leader_setup() ->
    #metadata{name=n1, nodes=[n2, n3], term=7, votes=[n1, n2], voted_for=n1}.

leader_options() ->
    [{timeout, ?HEARTBEAT_TIMEOUT, ticker}].

test_leader_timeout(#metadata{}=Metadata) ->
    Result = leader(timeout, ticker, Metadata),

    [
     ?_assertEqual(
        {keep_state_and_data, leader_options()},
        Result)
    ].

test_leader_vote_request_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #vote_request{term=Term-1, candidate_id=n2}, Metadata),

    [
     ?_assertEqual(
        {keep_state_and_data, leader_options()},
        Result)
    ].

test_leader_vote_request_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #vote_request{term=Term+1, candidate_id=n2}, Metadata),

    [
     ?_assertEqual(
        {next_state, follower, #metadata{name=n1, nodes=[n2, n3], term=Term+1, votes=[], voted_for=n2}, leader_options()},
        Result)
    ].

test_leader_vote_granted(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #vote_granted{term=Term, voter_id=n2}, Metadata),

    [
     ?_assertEqual(
        {keep_state_and_data, leader_options()},
        Result)
    ].

test_leader_heartbeat_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #append_entries{term=Term-1, leader_id=n2, entries=[]}, Metadata),

    [
     ?_assertEqual(
        {keep_state_and_data, leader_options()},
        Result)
    ].

test_leader_heartbeat_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #append_entries{term=Term+1, leader_id=n2, entries=[]}, Metadata),

    [
     ?_assertEqual(
        {next_state, follower, #metadata{name=n1, nodes=[n2, n3], term=Term+1, votes=[], voted_for=null}, leader_options()},
        Result)
    ].


leader_test_() ->
    [
     {
       "Leader times out and sends heartbeats",
       {setup, fun leader_setup/0, fun test_leader_timeout/1}
     },
     {
       "Leader receives a vote request but with an older term and ignores the request",
       {setup, fun leader_setup/0, fun test_leader_vote_request_with_older_term/1}
     },
     {
       "Leader receives a vote request but with the newer term and steps down to follower",
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
     }
    ].

%% -endif.
