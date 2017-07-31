-module(raft).

-behaviour(gen_statem).

%% API
-export([start/0, start_link/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([follower/3, candidate/3, leader/3]).

-export([get_timeout_options/0, get_timeout_options/1]).

-define(SERVER, ?MODULE).

-record(metadata, {name, nodes, term, votes = [], voted = false}).

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

follower(cast, #vote_request{candidate_id=CandidateId}=VoteRequest, #metadata{name=Name, voted=Voted}=Data) ->
    io:format("~p: Received vote request from: ~p~n", [Name, CandidateId]),

    case Voted of
        true ->
            io:format("~p: Already voted~n", [Name]),
            {keep_state, with_latest_term(VoteRequest, Data), [get_timeout_options()]};
        false ->
            IsValidElection = is_valid_election(VoteRequest, Data),
            case IsValidElection of
                true ->
                    send_vote(Name, VoteRequest),
                    io:format("~p: Vote sent to ~p~n", [Name, CandidateId]);
                false ->
                    io:format("~p: Vote denied to ~p~n", [Name, CandidateId])
            end,
            {keep_state, with_latest_term(VoteRequest, Data#metadata{voted=IsValidElection}), [get_timeout_options()]}
    end;

follower(cast, #vote_granted{}, #metadata{name=Name}) ->
    io:format("~p: Received vote in follower state~n", [Name]),
    {keep_state_and_data, [get_timeout_options()]};

follower(cast,
         #append_entries{leader_id=LeaderId, entries=Entries},
         #metadata{name=Name}=Data) ->
    case Entries of
        [] ->
            io:format("~p: Received heartbeat from ~p~n", [Name, LeaderId]),
            {keep_state, Data#metadata{voted=false}, [get_timeout_options()]}
    end.

candidate(timeout, ticker, #metadata{name=Name, term=Term}=Data) ->
    io:format("~p: starting election~n", [Name]),
    start_election(Data),
    {next_state, candidate, Data#metadata{term=Term+1, votes=[Name], voted=true}, [get_timeout_options()]};

candidate(cast,
          #vote_request{candidate_id=CandidateId}=VoteRequest,
          #metadata{name=Name}=Data) ->
    io:format("~p: Received vote request in candidate state~n", [Name]),

    case should_step_down(VoteRequest, Data) of
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
            {next_state, follower, Data#metadata{votes=[], voted=false}, [get_timeout_options()]}
    end.


leader(timeout, ticker, #metadata{term=Term, name=Name, nodes=Nodes}) ->
    io:format("~p: Leader timeout, sending Heartbeat~n", [Name]),
    Heartbeat = #append_entries{term=Term, leader_id=Name},
    [send_heartbeat(Node, Heartbeat) || Node <- Nodes],
    {keep_state_and_data, [get_timeout_options()]};

leader(cast, #vote_request{}=VoteRequest, #metadata{name=Name}=Data) ->
    io:format("~p: Received vote request in leader state~n", [Name]),
    case should_step_down(VoteRequest, Data) of
        true ->
            io:format("~p: Stepped down and voted~n", [Name]),
            send_vote(Name, VoteRequest),
            {next_state,
             follower,
             with_latest_term(VoteRequest, Data#metadata{votes=[], voted=false}),
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


is_valid_election(#vote_request{term=CandidateTerm}, #metadata{term=Term}) ->
    if CandidateTerm >= Term ->
            true;
       CandidateTerm < Term ->
            false
    end.

with_latest_term(#vote_request{term=CandidateTerm}, #metadata{term=CurrentTerm}=Data) ->
    if CandidateTerm >= CurrentTerm ->
            Data#metadata{term=CandidateTerm};
       CandidateTerm < CurrentTerm ->
            Data
    end.

should_step_down(#vote_request{term=CandidateTerm}, #metadata{term=Term}) ->
    if CandidateTerm > Term ->
            true;
       CandidateTerm =< Term ->
            false
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


-include_lib("eunit/include/eunit.hrl").


test_timeout_value(Timeout) ->
     ?_assert(Timeout < 3150).

test_get_timeout_options_arity_0() ->
    {timeout, Timeout, ticker} = get_timeout_options(),
    test_timeout_value(Timeout).

get_timeout_options_test_() ->
    [test_get_timeout_options_arity_0(),
     ?_assertEqual(get_timeout_options(10), {timeout, 3010, ticker})].


%%%===================================================================
%% Tests for state machine callbacks
%%%===================================================================

test_init_types() ->
    {ok,
     follower,
     #metadata{name=test, nodes=[n1, n2, n3], term=0, votes=[], voted=false},
     [{timeout, Timeout, ticker}]} = init([test]),

    test_timeout_value(Timeout).

init_test_() ->
    [test_init_types()].
