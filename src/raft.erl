-module(raft).

-include("raft.hrl").

-behaviour(gen_statem).

%% API
-export([start/0, start_link/1, stop/1, shutdown/0]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([follower/3, candidate/3, leader/3]).

-export([get_timeout_options/0, get_timeout_options/1, create_metadata/2]).

-define(SERVER, ?MODULE).


%%%===================================================================
%%% Public API
%%%===================================================================
start() ->
    [raft:start_link(Name) || #raft_node{name = Name} <- ?NODES].


start_link(Name) ->
    gen_statem:start_link({local, Name}, ?MODULE, [Name], []).

stop(Name) ->
    gen_statem:stop(Name).

shutdown() ->
    [raft:stop(Name) || #raft_node{name = Name} <- ?NODES].

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> state_functions.

-spec init(Args :: term()) ->
                  gen_statem:init_result(atom()).
init([Name]) ->
    Nodes = lists:delete(#raft_node{name = Name}, ?NODES),
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

follower(timeout, ticker, #metadata{name = Name} = Data) when is_atom(Name) ->
    %% Start an election
    log("timeout", Data, []),
    {
        next_state,
        candidate,
        Data#metadata{votes = [], voted_for = null},
        [get_timeout_options(0)]
    };

follower(cast,
         #vote_request{candidate_id = CandidateId} = VoteRequest,
         #metadata{name = Name, voted_for = null} = Data) ->
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
        with_latest_term(VoteRequest, Data#metadata{voted_for = VotedFor}),
        [get_timeout_options()]
    };

follower(cast,
         #vote_request{term = Term, candidate_id = CandidateId} = VoteRequest,
         #metadata{name = Name, term = CurrentTerm, voted_for = _VotedFor} = Data) ->
    log("Received vote request from: ~p for term: ~p", Data, [CandidateId, Term]),
    UpdatedData = if
                      Term > CurrentTerm ->
                          send_vote(Name, VoteRequest),
                          log("Vote sent to ~p", Data, [CandidateId]),
                          Data#metadata{term = Term, voted_for = CandidateId};
                      Term =< CurrentTerm ->
                          log("Vote denied to ~p, outdated request",
                              Data, [CandidateId]),
                          Data
                  end,

    {keep_state, UpdatedData, [get_timeout_options()]};

follower(cast, #vote_granted{}, #metadata{} = Data) ->
    log("Received vote in follower state, ignoring", Data, []),
    {keep_state_and_data, [get_timeout_options()]};

follower(cast,
         #append_entries{term = Term, leader_id = LeaderId, entries = []},
         #metadata{term = CurrentTerm} = Data) ->

    case is_valid_term(Term, CurrentTerm) of
        true ->
            log("Received heartbeat from ~p", Data, [LeaderId]),
            {keep_state, Data#metadata{leader_id = LeaderId}, [get_timeout_options()]};
        false ->
            log("Received heartbeat from ~p but it has outdated term",
                Data, [LeaderId]),
            {next_state, candidate, Data, [get_timeout_options(0)]}
    end;

follower(cast,
         #append_entries{},
         #metadata{}) ->
    {keep_state_and_data, [get_timeout_options()]};

follower(cast,
         #client_message{} = ClientMessage,
         #metadata{} = Data) ->

    send_leader_info(ClientMessage, Data),
    {keep_state_and_data, [get_timeout_options()]};

follower(Event, EventContext, Data) ->
    handle_event(Event, EventContext, Data).


candidate(timeout, ticker, #metadata{name = Name, term = Term} = Data) ->

    UpdatedData = Data#metadata{term = Term+1, votes = [Name], voted_for = Name},
    log("starting election", UpdatedData, []),
    start_election(UpdatedData),
    {keep_state, UpdatedData, [get_timeout_options()]};

candidate(cast,
          #vote_request{candidate_id = CandidateId} = VoteRequest,
          #metadata{name = Name} = Data) ->
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
                                 Data#metadata{votes = [], voted_for = CandidateId}),
                [get_timeout_options()]
            };
        false ->
            {keep_state_and_data, [get_timeout_options()]}
    end;

candidate(cast,
          #vote_granted{term = Term, voter_id = Voter},
          #metadata{nodes = Nodes, term = CurrentTerm, votes = Votes} = Data) ->

    case is_valid_term(Term, CurrentTerm) of
        false ->
            {keep_state_and_data, [get_timeout_options()]};
        true ->
            UpdatedVotes = lists:append(Votes, [Voter]),
            log("Current votes for candidate ~p", Data, [UpdatedVotes]),
            case has_majority(UpdatedVotes, Nodes) of
                true ->
                    log("Elected as Leader", Data, []),

                    %% next_index for each node should be reset to 0
                    %% when the candidate is promoted as a leader. The
                    %% leader uses this to set the value of next_index
                    %% to the immediate next after the last log entry
                    %% in its log.
                    UpdatedNodes = [Node#raft_node{next_index = 0} || Node <- Nodes],
                    {
                        next_state,
                        leader,
                        Data#metadata{votes = UpdatedVotes, nodes = UpdatedNodes},
                        [get_timeout_options(0)]
                    };
                false ->
                    {
                        keep_state,
                        Data#metadata{votes = UpdatedVotes},
                        [get_timeout_options()]
                    }
            end
    end;

candidate(cast,
          #append_entries{term = Term, leader_id = LeaderId, entries = []},
          #metadata{term = CurrentTerm} = Data) ->
    case is_valid_term(Term, CurrentTerm) of
        true ->
            log("Received heartbeat from ~p in candidate state with new term."
                " Stepping down", Data, [LeaderId]),
            {
                next_state,
                follower,
                Data#metadata{
                    term = Term,
                    votes = [],
                    voted_for = null,
                    leader_id = LeaderId
                   },
                [get_timeout_options()]
            };
        false ->
            {keep_state_and_data, [get_timeout_options()]}
    end;

candidate(cast,
          #append_entries{},
          #metadata{}) ->
    {keep_state_and_data, [get_timeout_options()]};

candidate(cast,
          #client_message{} = ClientMessage,
          #metadata{} = Data) ->

    send_leader_info(ClientMessage, Data),
    {keep_state_and_data, [get_timeout_options()]};

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
       #vote_request{candidate_id = CandidateId} = VoteRequest,
       #metadata{name = Name} = Data) ->
    log("Received vote request in leader state", Data, []),
    case can_grant_vote(VoteRequest, Data) of
        true ->
            log("Stepped down and voted", Data, []),
            send_vote(Name, VoteRequest),
            {
                next_state,
                follower,
                with_latest_term(VoteRequest,
                                 Data#metadata{votes = [], voted_for = CandidateId}),
                [get_timeout_options(?HEARTBEAT_TIMEOUT)]
            };
        false ->
            {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]}
    end;

leader(cast, #vote_granted{}, #metadata{} = Data) ->
    log("Received vote granted in leader state", Data, []),
    {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]};

leader(cast,
       #append_entries{term = Term, leader_id = LeaderId, entries = []},
       #metadata{term = CurrentTerm} = Data) ->
    case is_valid_term(Term, CurrentTerm) of
        true ->
            {
                next_state,
                follower,
                Data#metadata{
                    term = Term,
                    votes = [],
                    voted_for = null,
                    leader_id = LeaderId
                   },
                [get_timeout_options(?HEARTBEAT_TIMEOUT)]
            };
        false ->
            {keep_state_and_data, [get_timeout_options(?HEARTBEAT_TIMEOUT)]}
    end;

leader(cast,
       #client_message{command = Command} = ClientMessage,
       #metadata{
            term = Term,
            nodes = Nodes,
            log = Log,
            to_reply = ToReply} = Data) ->

    LatestLogEntry = get_latest_log_entry(Log),
    NewLogEntry = #log_entry{
                       index = LatestLogEntry#log_entry.index + 1,
                       term = Term,
                       command = Command},

    UpdatedLog = lists:append(Log, [NewLogEntry]),
    UpdatedToReply = lists:append(ToReply, [ClientMessage]),
    UpdatedData = Data#metadata{
                      log = UpdatedLog,
                      to_reply = UpdatedToReply},

    [send_append_entries(Node,
                         LatestLogEntry,
                         [NewLogEntry],
                         UpdatedData)
     || Node <- Nodes],

    send_response_to_client(ClientMessage, Data),

    {keep_state, UpdatedData, [get_timeout_options(?HEARTBEAT_TIMEOUT)]};

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


-spec get_timeout_options() -> {timeout, integer(), ticker}.
get_timeout_options() ->
    {timeout, Timeout, ticker} = get_timeout_options(rand:uniform(?TIMEOUT_RANGE)),
    {timeout, ?TIMEOUT_SEED + Timeout, ticker}.

-spec get_timeout_options(integer()) -> {timeout, integer(), ticker}.
get_timeout_options(Time) ->
    {timeout, Time, ticker}.


-spec log(string(), metadata(), list()) -> ok.
log(Message, #metadata{name = Name, term = Term}, Args) ->
    FormattedMessage = io_lib:format(Message, Args),
    io:format("[~p Term #~p]: ~s~n", [Name, Term, FormattedMessage]).


-spec has_majority(list(), [raft_node()]) -> boolean().
has_majority(Votes, Nodes) when is_list(Votes), is_list(Nodes) ->
    length(Votes) >= (length(Nodes) div 2) + 1.


-spec start_election(metadata()) -> list().
start_election(#metadata{name = Name, nodes = Nodes, term = Term}) ->
    VoteRequest = #vote_request{term = Term, candidate_id = Name},
    [request_vote(Voter, VoteRequest) || #raft_node{name = Voter} <- Nodes].


-spec request_vote(atom(), vote_request()) -> ok.
request_vote(Voter, VoteRequest)
  when is_atom(Voter) ->
    gen_statem:cast(Voter, VoteRequest).


-spec can_grant_vote(vote_request(), metadata()) -> boolean().
can_grant_vote(#vote_request{term = CandidateTerm}, #metadata{term = CurrentTerm})
  when is_integer(CandidateTerm), is_integer(CurrentTerm) ->
    CandidateTerm >= CurrentTerm.


-spec is_valid_term(integer(), integer()) -> boolean().
is_valid_term(Term, CurrentTerm)
  when is_integer(Term), is_integer(CurrentTerm) ->
    Term >= CurrentTerm.


-spec with_latest_term(vote_request(), metadata()) -> metadata().
with_latest_term(#vote_request{term = CandidateTerm},
                 #metadata{term = CurrentTerm} = Data) ->
    if CandidateTerm >= CurrentTerm ->
            Data#metadata{term = CandidateTerm};
       CandidateTerm < CurrentTerm ->
            Data
    end.


-spec get_latest_log_entry([log_entry()]) -> log_entry().
get_latest_log_entry([]) ->
    #log_entry{index = 0, term = 0};

get_latest_log_entry(Log) ->
    lists:last(Log).


-spec send_vote(atom(), vote_request()) -> ok.
send_vote(Name, #vote_request{term = Term, candidate_id = CandidateId}) ->
    VoteGranted = #vote_granted{term = Term, voter_id = Name},
    gen_statem:cast(CandidateId, VoteGranted).


-spec send_leader_info(
          client_message(),
          metadata()
         ) -> {error, atom(), string(), atom()}.
send_leader_info(#client_message{client_id = To, message_id = MessageId},
                 #metadata{name = From, leader_id = LeaderId}) ->
    io:format("Sending leader info to client"),
    To ! {error, From, MessageId, LeaderId}.


-spec send_heartbeat(raft_node(), append_entries()) -> ok.
send_heartbeat(#raft_node{name = Name}, Heartbeat) ->
    gen_statem:cast(Name, Heartbeat).


-spec send_response_to_client(
          client_message(),
          metadata()
         ) -> {ok, atom(), string()}.
send_response_to_client(#client_message{client_id = To, message_id = MessageId},
                        #metadata{name = From}) ->
    To ! {ok, From, MessageId}.


-spec send_append_entries(
          raft_node(),
          log_entry(),
          [log_entry()],
          metadata()
         ) -> ok.
send_append_entries(#raft_node{name = Name},
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

    gen_statem:cast(Name, AppendEntries),
    ok.
