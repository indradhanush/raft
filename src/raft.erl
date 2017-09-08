-module(raft).

-include("raft.hrl").

-behaviour(gen_statem).

%% API
-export([start/0, start_link/1, stop/1, shutdown/0]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([follower/3, candidate/3, leader/3]).

-define(SERVER, ?MODULE).

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
                #raft_node{name=n1},
                #raft_node{name=n2},
                #raft_node{name=n3},
                #raft_node{name=n4},
                #raft_node{name=n5}
               ]).

-else.

-define(TIMEOUT_SEED, 0).

-define(NODES, [
                #raft_node{name=n1},
                #raft_node{name=n2},
                #raft_node{name=n3},
                #raft_node{name=n4},
                #raft_node{name=n5}
               ]).

-endif.


-record(vote_request, {term, candidate_id}).

-record(vote_granted, {term, voter_id}).

-record(append_entries, {
            term           :: non_neg_integer(),
            leader_id      :: atom(),
            prev_log_index :: non_neg_integer(),
            prev_log_term  :: non_neg_integer(),
            entries = []   :: [log_entry()]
           }).


%%%===================================================================
%%% Public API
%%%===================================================================
start() ->
    [raft:start_link(Name) || #raft_node{name=Name} <- ?NODES].


start_link(Name) ->
    gen_statem:start_link({local, Name}, ?MODULE, [Name], []).

stop(Name) ->
    gen_statem:stop(Name).

shutdown() ->
    [raft:stop(Name) || #raft_node{name=Name} <- ?NODES].

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> state_functions.

-spec init(Args :: term()) ->
                  gen_statem:init_result(atom()).
init([Name]) ->
    Nodes = lists:delete(#raft_node{name=Name}, ?NODES),
    Data = create_metadata(Name, Nodes),
    log("Initiating with nodes ~p", Data, [Nodes]),
    {
        ok,
        follower,
        Data,
        [get_timeout_options()]
    }.

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
    {
        next_state,
        candidate,
        Data#metadata{votes=[], voted_for=null},
        [get_timeout_options(0)]
    };

follower(cast,
         #vote_request{candidate_id=CandidateId}=VoteRequest,
         #metadata{name=Name, voted_for=null}=Data) ->
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
    {
        keep_state,
        with_latest_term(VoteRequest, Data#metadata{voted_for=VotedFor}),
        [get_timeout_options()]
    };

follower(cast,
         #vote_request{term=Term, candidate_id=CandidateId}=VoteRequest,
         #metadata{name=Name, term=CurrentTerm, voted_for=_VotedFor}=Data) ->
    log("Received vote request from: ~p, but already voted", Data, [CandidateId]),
    UpdatedData = if
                      Term > CurrentTerm ->
                          send_vote(Name, VoteRequest),
                          log("Vote sent to ~p", Data, [CandidateId]),
                          Data#metadata{term=Term, voted_for=CandidateId};
                      Term =< CurrentTerm ->
                          log("Vote denied to ~p, outdated request",
                              Data, [CandidateId]),
                          Data
                  end,

    {keep_state, UpdatedData, [get_timeout_options()]};

follower(cast, #vote_granted{}, #metadata{}=Data) ->
    log("Received vote in follower state, ignoring", Data, []),
    {keep_state_and_data, [get_timeout_options()]};

follower(cast,
         #append_entries{term=Term, leader_id=LeaderId, entries=[]},
         #metadata{term=CurrentTerm}=Data) ->

    case is_valid_term(Term, CurrentTerm) of
        true ->
            log("Received heartbeat from ~p", Data, [LeaderId]),
            {keep_state, Data#metadata{leader_id=LeaderId}, [get_timeout_options()]};
        false ->
            log("Received heartbeat from ~p but it has outdated term", Data, [LeaderId]),
            {next_state, candidate, Data, [get_timeout_options(0)]}
    end;

follower({call, From}, #client_message{}, #metadata{leader_id=LeaderId}) ->
    Reply = {error, LeaderId},
    {keep_state_and_data, [get_timeout_options(), {reply, From, Reply}]};

follower(Event, EventContext, Data) ->
    handle_event(Event, EventContext, Data).


candidate(timeout, ticker, #metadata{name=Name, term=Term}=Data) ->

    UpdatedData = Data#metadata{term=Term+1, votes=[Name], voted_for=Name},
    log("starting election", UpdatedData, []),
    start_election(UpdatedData),
    {keep_state, UpdatedData, [get_timeout_options()]};

candidate(cast,
          #vote_request{candidate_id=CandidateId}=VoteRequest,
          #metadata{name=Name}=Data) ->
    log("Received vote request in candidate state", Data, []),

    case can_grant_vote(VoteRequest, Data) of
        true ->
            log("Candidate stepping down. Received vote request for a new term from ~p",
                Data, [CandidateId]),
            send_vote(Name, VoteRequest),
            {
                next_state,
                follower,
                with_latest_term(VoteRequest,
                                 Data#metadata{votes=[], voted_for=CandidateId}),
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
            {
                next_state,
                follower,
                Data#metadata{term=Term, votes=[], voted_for=null, leader_id=LeaderId},
                [get_timeout_options()]
            };
        false ->
            {keep_state_and_data, [get_timeout_options()]}
    end;

candidate({call, From}, #client_message{}, #metadata{leader_id=LeaderId}) ->
    Reply = {error, LeaderId},
    {keep_state_and_data, [get_timeout_options(), {reply, From, Reply}]};

candidate(Event, EventContext, Data) ->
    handle_event(Event, EventContext, Data).


leader(timeout,
       ticker,
       #metadata{term = Term, name = Name, nodes = Nodes, log = Log} = Data) ->

    log("Leader timeout, sending Heartbeat", Data, []),

    LatestLogEntry = get_latest_log_entry(Log),

    NextIndex = LatestLogEntry#log_entry.index + 1,

    %% If the leader timed out as a result of being elected (candidate
    %% sets a timeout of 0 when it changes state to leader),
    %% #raft_node.next_index is initialized to 0. We update that value
    %% to the index of the LatestLogEntry + 1 ; Or else, the leader
    %% has been active for some time and we do not want to mess with
    %% the next_index state it had maintained for each node.
    %%
    %% This makes it important that the state is reset when a leader
    %% steps down and initialized with the default value of next_index
    %% for each node when a candidate promotes itself to leader.
    UpdatedNodes = lists:map(
                       fun(#raft_node{next_index = CurrentNextIndex} = Node) ->
                           case CurrentNextIndex of
                               0 -> Node#raft_node{next_index = NextIndex};
                               _ -> Node
                           end
                       end,
                       Nodes),

    Heartbeat = #append_entries{term = Term, leader_id = Name},

    [send_heartbeat(Node, Heartbeat) || Node <- Nodes],

    {
        keep_state,
        Data#metadata{nodes = UpdatedNodes},
        [get_timeout_options(?HEARTBEAT_TIMEOUT)]
    };

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
                with_latest_term(VoteRequest,
                                 Data#metadata{votes=[], voted_for=CandidateId}),
                [get_timeout_options(?HEARTBEAT_TIMEOUT)]
            };
        false ->
            {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]}
    end;

leader(cast, #vote_granted{}, #metadata{}=Data) ->
    log("Received vote granted in leader state", Data, []),
    {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]};

leader(cast,
       #append_entries{term=Term, leader_id=LeaderId, entries=[]},
       #metadata{term=CurrentTerm}=Data) ->
    case is_valid_term(Term, CurrentTerm) of
        true ->
            {
                next_state,
                follower,
                Data#metadata{term=Term, votes=[], voted_for=null, leader_id=LeaderId},
                [get_timeout_options(?HEARTBEAT_TIMEOUT)]
            };
        false ->
            {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]}
    end;

leader({call, From},
       #client_message{command=Command},
       #metadata{term=Term, nodes=Nodes, log=Log}=Data) ->
    %% TODO: The awesome atom is a place holder to differentiate
    %% between the response of a follower and candidate from a
    %% leader. When we implement log replication, this will be the
    %% index of the log instead. But fear not, we have tests for this
    %% that assert for awesome being returned which will fail when we
    %% make the changes here. Win win.
    Index = case Log of
        [] ->
            1;
        _ ->
            #log_entry{index = LastIndex} = lists:last(Log),
            LastIndex + 1
    end,

    NewLogEntry = #log_entry{
                    index = Index,
                    term = Term,
                    command = Command
                   },
    UpdatedLog = lists:append(Log, [NewLogEntry]),
    UpdatedData = Data#metadata{log = UpdatedLog},

    [send_append_entries(Node, UpdatedData) || Node <- Nodes],

    Reply = {ok, awesome},
    {keep_state, UpdatedData, [get_timeout_options(), {reply, From, Reply}]};

leader(Event, EventContext, Data) ->
    handle_event(Event, EventContext, Data).


%% This is required to trigger a manual timeout in our tests since we
%% use an abnormally high number for timeouts in tests to make things
%% deterministic and easy to tests. So far we have been unable to find
%% a way to trigger a timeout manually thus requiring this function.
handle_event(cast, test_timeout, #metadata{}) ->
    {keep_state_and_data, [get_timeout_options(0)]}.


-spec terminate(
          Reason :: term(),
          State :: term(),
          Data :: term()) ->
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

-spec create_metadata(atom(), [raft_node()]) -> metadata().
create_metadata(Name, Nodes) ->
    #metadata{name = Name, nodes = Nodes}.

get_timeout_options() ->
    {timeout, Timeout, ticker} = get_timeout_options(rand:uniform(?TIMEOUT_RANGE)),
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
    [request_vote(Voter, VoteRequest) || #raft_node{name=Voter} <- Nodes].


request_vote(Voter, VoteRequest)
  when is_atom(Voter) ->
    gen_statem:cast(Voter, VoteRequest).


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

-spec get_latest_log_entry([log_entry()]) -> log_entry().
get_latest_log_entry([]) ->
    #log_entry{index = 0, term = 0};

get_latest_log_entry(Log) ->
    lists:last(Log).


send_vote(Name, #vote_request{term=Term, candidate_id=CandidateId}) ->
    VoteGranted = #vote_granted{term=Term, voter_id=Name},
    gen_statem:cast(CandidateId, VoteGranted).


send_heartbeat(#raft_node{name=Name}, Heartbeat) ->
    gen_statem:cast(Name, Heartbeat).


-spec send_append_entries(
          atom(),
          log_entry(),
          [log_entry()],
          metadata()
         ) -> ok.

send_append_entries(Node,
                    #log_entry{index = PrevLogIndex, term = PrevLogTerm},
                    LogEntries,
                    #metadata{name = LeaderId, term = Term}) ->

    AppendEntries = #append_entries{
                         term = Term,
                         leader_id = LeaderId,
                         prev_log_index = PrevLogIndex,
                         prev_log_term = PrevLogTerm,
                         entries = LogEntries
                        },

    gen_statem:cast(Node, AppendEntries),
    ok.


%%%===================================================================
%% Tests for internal functions
%%%===================================================================

%% -ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


assert_options([{timeout, Timeout, ticker}]) ->
    [?_assert(Timeout >= ?TIMEOUT_SEED),
     ?_assert(Timeout =< ?TIMEOUT_SEED + ?TIMEOUT_RANGE)].

test_get_timeout_options_arity_0() ->
    assert_options([get_timeout_options()]).


get_timeout_options_test_() ->
    [test_get_timeout_options_arity_0(),
     ?_assertEqual(get_timeout_options(10), {timeout, 10, ticker})].


%%%===================================================================
%% Tests for state machine callbacks
%%%===================================================================

test_init_types() ->
    Result = init([n1]),
    {_, _, _, Options} = Result,

    Expected = {
        ok,
        follower,
        create_metadata(
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
    Nodes = lists:delete(#raft_node{name=n1}, ?NODES),
    create_metadata(n1, Nodes).

follower_setup() ->
    Metadata = initial_metadata(),
    Metadata#metadata{term = 5}.

test_follower_timeout(#metadata{term=Term}=Metadata) ->
    Result = follower(timeout, ticker, Metadata#metadata{voted_for=n2}),

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        candidate,
        ExpectedMetadata#metadata{term = Term},
        [{timeout, 0, ticker}]
       },

    [?_assertEqual(Expected, Result)].

test_follower_vote_request_not_voted_yet(#metadata{term=Term}=Metadata) ->
    Result = follower(cast,
                      #vote_request{term=Term+1, candidate_id=n2},
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

test_follower_vote_request_with_candidate_older_term(#metadata{term=Term}=Metadata) ->
    Result = follower(cast,
                      #vote_request{term=Term-1, candidate_id=n2},
                      Metadata#metadata{voted_for=n3}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term, voted_for = n3},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_vote_request_with_already_voted(#metadata{term=Term}=Metadata) ->
    Result = follower(cast,
                      #vote_request{term=Term, candidate_id=n2},
                      Metadata#metadata{voted_for=n3}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term, voted_for = n3},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_vote_request_with_new_term_but_already_voted(#metadata{term=Term}=Metadata) ->
    Result = follower(cast,
                      #vote_request{term=Term+1, candidate_id=n2},
                      Metadata#metadata{voted_for=n3}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term = Term+1, voted_for=n2},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_vote_granted(#metadata{}=Metadata) ->
    Result = follower(cast, #vote_granted{}, Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_heartbeat_just_after_voting(#metadata{term=Term}=Metadata) ->
    Result = follower(cast,
                      #append_entries{term=Term, leader_id=n2},
                      Metadata#metadata{voted_for=n2}),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term=Term, votes=[], voted_for=n2, leader_id=n2},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_follower_heartbeat_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = follower(cast,
                      #append_entries{term=Term-1, leader_id=n2},
                      Metadata#metadata{voted_for=n3, leader_id=n3}),
    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        candidate,
        ExpectedMetadata#metadata{term=Term, votes=[], voted_for=n3, leader_id=n3},
        [{timeout, 0, ticker}]
       },

    [?_assertEqual(Expected, Result)].

test_follower_call(#metadata{}=Metadata) ->
    Result = follower({call, client}, #client_message{}, Metadata#metadata{leader_id=n2}),
    {_, [TimeoutOptions, _]} = Result,

    Expected = {keep_state_and_data, [TimeoutOptions, {reply, client, {error, n2}}]},

    [assert_options([TimeoutOptions]),
     ?_assertEqual(Expected, Result)].


follower_test_() ->
    [
     {
         "Follower promotes itself to candidate if times out",
         {setup, fun follower_setup/0, fun test_follower_timeout/1}
     },
     {
         "Follower received a vote request but has not voted in yet and votes in favour",
         {setup, fun follower_setup/0, fun test_follower_vote_request_not_voted_yet/1}
     },
     {
         "Follower received a vote request but candidate has an older term",
         {setup, fun follower_setup/0, fun test_follower_vote_request_with_candidate_older_term/1}
     },
     {
         "Follower received a vote request but follower has already voted for this term",
         {setup, fun follower_setup/0, fun test_follower_vote_request_with_already_voted/1}
     },
     {
         "Follower received a vote request but follower has already voted in the previous term,
        however, this is a new term and it votes again in favour of the new candidate",
       {setup, fun follower_setup/0, fun test_follower_vote_request_with_new_term_but_already_voted/1}
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
         "Follower received a heartbeat but from with an older term and promotes itself to candidate",
         {setup, fun follower_setup/0, fun test_follower_heartbeat_with_older_term/1}
     },
     {
         "Follower replies to a call request with error and leader id",
         {setup, fun follower_setup/0, fun test_follower_call/1}
     }
    ].


candidate_setup() ->
    Metadata = initial_metadata(),
    Metadata#metadata{term = 6, votes = [n1], voted_for = n1}.

test_candidate_timeout(#metadata{term=Term}=Metadata) ->
    Result = candidate(timeout, ticker, Metadata),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term=Term+1, votes=[n1], voted_for=n1},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_request_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #vote_request{term=Term-1, candidate_id=n2}, Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_request_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #vote_request{term=Term+1, candidate_id=n2}, Metadata),
    {_, _, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{term=Term+1, votes=[], voted_for=n2},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_granted_but_no_majority(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast,
                       #vote_granted{term=Term, voter_id=n2},
                       Metadata),
    {_, _, Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        keep_state,
        ExpectedMetadata#metadata{term=Term, votes=[n1, n2], voted_for=n1},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_vote_granted_with_majority(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast,
                       #vote_granted{term=Term, voter_id=n3},
                       Metadata#metadata{votes = [n1, n2]}),
    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state, leader,
        ExpectedMetadata#metadata{term=Term, votes=[n1, n2, n3], voted_for=n1},
        [{timeout, 0, ticker}]
       },

    [?_assertEqual(Expected, Result)].

test_candidate_vote_granted_but_older_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #vote_granted{term=Term-1, voter_id=n2}, Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_heartbeat_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast,
                       #append_entries{term=Term-1, leader_id=n2},
                       Metadata),
    {_, Options} = Result,

    Expected = {keep_state_and_data, Options},

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_heartbeat_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = candidate(cast, #append_entries{term=Term+1, leader_id=n2}, Metadata),
    {_, _, _,  Options} = Result,

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{term=Term+1, votes=[], voted_for=null, leader_id=n2},
        Options
       },

    [assert_options(Options),
     ?_assertEqual(Expected, Result)].

test_candidate_call(#metadata{}=Metadata) ->
    Result = candidate({call, client}, #client_message{}, Metadata#metadata{leader_id=n2}),
    {_, [TimeoutOptions, _]} = Result,

    Expected = {keep_state_and_data, [TimeoutOptions, {reply, client, {error, n2}}]},

    [assert_options([TimeoutOptions]),
     ?_assertEqual(Expected, Result)].


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
     },
     {
         "Candidate replies to a call request with error and leader id",
         {setup, fun candidate_setup/0, fun test_candidate_call/1}
     }
    ].

leader_setup() ->
    Metadata = initial_metadata(),
    Metadata#metadata{term = 7, votes = [n1, n2], voted_for = n1}.

leader_options() ->
    [{timeout, ?HEARTBEAT_TIMEOUT, ticker}].

test_leader_timeout_with_empty_log(#metadata{term = Term} = Metadata) ->
    Result = leader(timeout, ticker, Metadata),

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

test_leader_vote_request_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #vote_request{term=Term-1, candidate_id=n2}, Metadata),

    Expected = {keep_state_and_data, leader_options()},

    [?_assertEqual(Expected, Result)].

test_leader_vote_request_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #vote_request{term=Term+1, candidate_id=n2}, Metadata),

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{term=Term+1, votes=[], voted_for=n2},
        leader_options()
       },

    [?_assertEqual(Expected, Result)].

test_leader_vote_granted(#metadata{term=Term}=Metadata) ->
    Result = leader(cast, #vote_granted{term=Term, voter_id=n2}, Metadata),

    Expected = {keep_state_and_data, leader_options()},

    [?_assertEqual(Expected, Result)].

test_leader_heartbeat_with_older_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast,
                    #append_entries{term=Term-1, leader_id=n2, entries=[]},
                    Metadata),

    Expected = {keep_state_and_data, leader_options()},

    [?_assertEqual(Expected, Result)].

test_leader_heartbeat_with_newer_term(#metadata{term=Term}=Metadata) ->
    Result = leader(cast,
                    #append_entries{term=Term+1, leader_id=n2, entries=[]},
                    Metadata),

    ExpectedMetadata = initial_metadata(),
    Expected = {
        next_state,
        follower,
        ExpectedMetadata#metadata{term=Term+1, votes=[], voted_for=null, leader_id=n2},
        leader_options()
       },

    [?_assertEqual(Expected, Result)].

test_leader_call(#metadata{term=Term}=Metadata) ->
    Result = leader({call, client}, #client_message{command = "test"}, Metadata),
    {_, _, [TimeoutOptions, _]} = Result,

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
                     }]
           },
        [TimeoutOptions, {reply, client, {ok, awesome}}]},

    [assert_options([TimeoutOptions]),
     ?_assertEqual(Expected, Result)].


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
     },
     {
         "Leader replies to a call request with ok and leader id",
         {setup, fun leader_setup/0, fun test_leader_call/1}
     }
    ].

%% -endif.
