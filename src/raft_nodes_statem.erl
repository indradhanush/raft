-module(raft_nodes_statem).

-behaviour(gen_statem).

%% API
-export([start/0, start_link/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([follower/3, candidate/3, leader/3]).

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

follower(timeout, ticker, #metadata{term=Term, name=Name}=Data) when is_atom(Name) ->
    %% Start an election
    io:format("~p: timeout~n", [Name]),
    {next_state, candidate, Data#metadata{term=Term+1}, [get_timeout_options(0)]};

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

follower(cast, #append_entries{leader_id=LeaderId, entries=Entries}, #metadata{name=Name}) ->
    case Entries of
        [] ->
            io:format("~p: Received heartbeat from ~p~n", [Name, LeaderId]),
            {keep_state_and_data, [get_timeout_options()]}
    end.

candidate(timeout, ticker, #metadata{name=Name}=Data) ->
    io:format("~p: starting election~n", [Name]),
    start_election(Data),
    {next_state, candidate, Data, [get_timeout_options()]};

candidate(cast, #vote_request{}, #metadata{name=Name}) ->
    io:format("~p: Received vote request in candidate state~n", [Name]),
    {keep_state_and_data, [get_timeout_options()]};

candidate(cast,
          #vote_granted{voter_id=Voter},
          #metadata{name=Name, nodes=Nodes, votes=Votes}=Data) ->

    UpdatedVotes = lists:append(Votes, [Voter]),
    case has_majority(length(Nodes), length(UpdatedVotes)) of
        true ->
            io:format("~p: Elected as Leader~n", [Name]),
            {next_state, leader, Data#metadata{votes=UpdatedVotes}, [get_timeout_options(0)]};
        false ->
            {keep_state, Data#metadata{votes=UpdatedVotes}, [get_timeout_options()]}
    end;

candidate(cast, #append_entries{leader_id=LeaderId, entries=Entries}, #metadata{name=Name}) ->
    case Entries of
        [] ->
            io:format("~p: Received heartbeat from ~p in candidate state~n", [Name, LeaderId]),
            {keep_state_and_data, [get_timeout_options()]}
    end.


leader(timeout, ticker, #metadata{term=Term, name=Name, nodes=Nodes}) ->
    io:format("~p: Leader timeout, sending Heartbeat~n", [Name]),
    Heartbeat = #append_entries{term=Term, leader_id=Name},
    [send_heartbeat(Node, Heartbeat) || Node <- Nodes],
    {keep_state_and_data, [get_timeout_options()]};

leader(cast, #vote_request{}, #metadata{name=Name}) ->
    io:format("~p: Received vote request in leader state~n", [Name]),
    {keep_state_and_data, [get_timeout_options()]};

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
    get_timeout_options(2000 + rand:uniform(150)).

get_timeout_options(Time) ->
    {timeout, Time, ticker}.


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

request_vote(Voter, VoteRequest) ->
    gen_statem:cast(Voter, VoteRequest).


send_vote(Name, #vote_request{term=Term, candidate_id=CandidateId}) ->
    VoteGranted = #vote_granted{term=Term, voter_id=Name},
    gen_statem:cast(CandidateId, VoteGranted).


send_heartbeat(Node, Heartbeat) ->
    gen_statem:cast(Node, Heartbeat).
