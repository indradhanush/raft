-module(raft).

-behaviour(gen_statem).

%% API
-export([start/0, start_link/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([follower/3, candidate/3, leader/3]).

-export([get_timeout_options/0, get_timeout_options/1]).

-define(SERVER, ?MODULE).

-record(metadata, {name, nodes, term, votes = [], voted_for = null}).

-record(vote_request, {term, candidate_id}).

-record(vote_granted, {term, voter_id}).

-record(append_entries, {term, leader_id, entries = []}).


%%%===================================================================
%%% Public API
%%%===================================================================
start() ->
    raft_nodes_statem:start_link(n1),
    raft_nodes_statem:start_link(n2),
    raft_nodes_statem:start_link(n3).


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
    io:format("~p: timeout~n", [Name]),
    {next_state, candidate, Data, [get_timeout_options(0)]};

follower(cast, #vote_request{candidate_id=CandidateId}=VoteRequest, #metadata{name=Name, voted_for=null}=Data) ->
    io:format("~p: Received vote request from: ~p~n", [Name, CandidateId]),
    VotedFor = case is_valid_term(VoteRequest, Data) of
                   true ->
                       send_vote(Name, VoteRequest),
                       io:format("~p: Vote sent to ~p~n", [Name, CandidateId]),
                       CandidateId;
                   false ->
                       io:format("~p: Vote denied to ~p~n", [Name, CandidateId]),
                       null
               end,
    {keep_state, with_latest_term(VoteRequest, Data#metadata{voted_for=VotedFor}), [get_timeout_options()]};

follower(cast, #vote_request{candidate_id=CandidateId}=VoteRequest, #metadata{name=Name}=Data) ->
    io:format("~p: Received vote request from: ~p~n", [Name, CandidateId]),
    io:format("~p: Already voted~n", [Name]),
    {keep_state, with_latest_term(VoteRequest, Data), [get_timeout_options()]};

follower(cast, #vote_granted{}, #metadata{name=Name}) ->
    io:format("~p: Received vote in follower state~n", [Name]),
    {keep_state_and_data, [get_timeout_options()]};

follower(cast,
         #append_entries{leader_id=LeaderId, entries=Entries},
         #metadata{name=Name}=Data) ->
    case Entries of
        [] ->
            io:format("~p: Received heartbeat from ~p~n", [Name, LeaderId]),
            {keep_state, Data#metadata{voted_for=null}, [get_timeout_options()]}
    end.

candidate(timeout, ticker, #metadata{name=Name, term=Term}=Data) ->
    io:format("~p: starting election~n", [Name]),
    start_election(Data),
    {next_state, candidate, Data#metadata{term=Term+1, votes=[Name], voted_for=Name}, [get_timeout_options()]};

candidate(cast,
          #vote_request{candidate_id=CandidateId}=VoteRequest,
          #metadata{name=Name}=Data) ->
    io:format("~p: Received vote request in candidate state~n", [Name]),

    case is_valid_term(VoteRequest, Data) of
        true ->
            io:format("~p: Candidate stepping down. Received vote request for a new term from ~p~n", [Name, CandidateId]),
            {next_state, follower, with_latest_term(VoteRequest, Data), [get_timeout_options()]};
        false ->
            {keep_state_and_data, [get_timeout_options()]}
    end;

candidate(cast,
          #vote_granted{voter_id=Voter},
          #metadata{name=Name, nodes=Nodes, votes=Votes}=Data) ->

    UpdatedVotes = lists:append(Votes, [Voter]),
    io:format("~p: Current votes for candidate ~p~n", [Name, UpdatedVotes]),
    case has_majority(length(Nodes), length(UpdatedVotes)) of
        true ->
            io:format("~p: Elected as Leader~n", [Name]),
            {next_state, leader, Data#metadata{votes=UpdatedVotes}, [get_timeout_options(0)]};
        false ->
            {keep_state, Data#metadata{votes=UpdatedVotes}, [get_timeout_options()]}
    end;

candidate(cast,
          #append_entries{leader_id=LeaderId, entries=Entries},
          #metadata{name=Name}=Data) ->
    case Entries of
        [] ->
            io:format("~p: Received heartbeat from ~p in candidate state~n", [Name, LeaderId]),
            {next_state, follower, Data#metadata{votes=[], voted_for=null}, [get_timeout_options()]}
    end.


leader(timeout, ticker, #metadata{term=Term, name=Name, nodes=Nodes}) ->
    io:format("~p: Leader timeout, sending Heartbeat~n", [Name]),
    Heartbeat = #append_entries{term=Term, leader_id=Name},
    [send_heartbeat(Node, Heartbeat) || Node <- Nodes],
    {keep_state_and_data, [get_timeout_options()]};

leader(cast, #vote_request{}=VoteRequest, #metadata{name=Name}=Data) ->
    io:format("~p: Received vote request in leader state~n", [Name]),
    case is_valid_term(VoteRequest, Data) of
        true ->
            io:format("~p: Stepped down and voted~n", [Name]),
            send_vote(Name, VoteRequest),
            {next_state,
             follower,
             with_latest_term(VoteRequest, Data#metadata{votes=[], voted_for=null}),
            [get_timeout_options()]};
        false ->
            {keep_state_and_data, [get_timeout_options()]}
    end;

leader(cast, #vote_granted{}, #metadata{name=Name}) ->
    io:format("~p: Received vote granted in leader state~n", [Name]),
    {keep_state_and_data, [get_timeout_options()]}.


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
    get_timeout_options(rand:uniform(150)).

get_timeout_options(Time) ->
    {timeout, 3000+Time, ticker}.


has_majority(LenNodes, LenVotes) when is_integer(LenNodes), is_integer(LenVotes) ->
    if LenVotes >= LenNodes ->
            true;
       LenVotes < LenNodes ->
            false
    end.

start_election(#metadata{name=Name, nodes=Nodes, term=Term}) ->
    VoteRequest = #vote_request{term=Term, candidate_id=Name},
    [request_vote(Voter, VoteRequest) || Voter <- Nodes].


is_valid_term(#vote_request{term=CandidateTerm}, #metadata{term=CurrentTerm}) ->
    CandidateTerm > CurrentTerm.


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
    [?_assert(Timeout >= 3000),
    ?_assert(Timeout < 3150)].

test_get_timeout_options_arity_0() ->
    assert_options([get_timeout_options()]).

%% timeout_options() ->
%%     {timeout, Timeout, ticker}.

get_timeout_options_test_() ->
    [test_get_timeout_options_arity_0(),
     ?_assertEqual(get_timeout_options(10), {timeout, 3010, ticker})].


%%%===================================================================
%% Tests for state machine callbacks
%%%===================================================================

test_init_types() ->
    {ok,
     follower,
     #metadata{name=test, nodes=[n1, n2, n3], term=0, votes=[], voted_for=null},
     Options} = init([test]),

    assert_options(Options).

init_test_() ->
    [test_init_types()].


follower_setup() ->
    #metadata{name=n1, nodes=[n2, n3], term=5}.

test_follower_timeout(#metadata{term=Term}=Metadata) ->
    Result = follower(timeout, ticker, Metadata),
    {_, _, _, Options} = Result,

    [assert_options(Options),
     ?_assertEqual(
        {next_state, candidate, #metadata{name=n1, nodes=[n2, n3], term=Term, votes=[], voted_for=null}, Options},
        Result)
     ].

test_follower_vote_request(#metadata{term=Term}=Metadata) ->
    VoteRequest = #vote_request{term=Term+1, candidate_id=n2},
    {keep_state,
     #metadata{voted_for=n2},
     Options} = follower(cast, VoteRequest, Metadata#metadata{}),

    [assert_options(Options)].

test_follower_vote_request_with_candidate_older_term(#metadata{term=Term}=Metadata) ->
    VoteRequest = #vote_request{term=Term, candidate_id=n2},
    {keep_state,
     #metadata{voted_for=null},
     Options} = follower(cast, VoteRequest, Metadata),

    [assert_options(Options)].

test_follower_vote_request_with_already_voted(#metadata{term=Term}=Metadata) ->
    VoteRequest = #vote_request{term=Term, candidate_id=n2},
    {keep_state,
     #metadata{voted_for=n3},
     Options} = follower(cast, VoteRequest, Metadata#metadata{voted_for=n3}),

    [assert_options(Options)].

test_follower_vote_granted(#metadata{}=Metadata) ->
    {keep_state_and_data, Options} = follower(cast, #vote_granted{}, Metadata),

    [assert_options(Options)].


follower_test_() ->
    [{"Follower promotes itself to candidate if times out",
      {setup, fun follower_setup/0, fun test_follower_timeout/1}},
     {"Follower received a vote request and votes in favour",
      {setup, fun follower_setup/0, fun test_follower_vote_request/1}},
     {"Follower received a vote request but candidate has an older term",
      {setup, fun follower_setup/0, fun test_follower_vote_request_with_candidate_older_term/1}},
     {"Follower received a vote request but follower has already voted",
      {setup, fun follower_setup/0, fun test_follower_vote_request_with_already_voted/1}},
     {"Follower received a vote granted but ignores it",
      {setup, fun follower_setup/0, fun test_follower_vote_granted/1}}
    ].



%% -endif.
