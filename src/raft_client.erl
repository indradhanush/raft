-module(raft_client).

-include("raft.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([write/2]).


write(Node, #client_message{} = Message) ->
    gen_statem:cast(Node, Message),
    io:format("~nWrite complete~n"),
    wait_for_response(Node, Message).

wait_for_response(Node, #client_message{message_id = MessageId}) ->
    receive
        {error, Node, MessageId, LeaderId} ->
            io:format("~nLeaderId is: ~p", [LeaderId]),
            {error, LeaderId};
        {ok, Node, MessageId} ->
            ok
    end.

    %%     {error, null} ->
    %%         {error, null};
    %%     {error, LeaderId} ->
    %%         write(LeaderId, Message);
    %%     {ok, awesome} ->
    %%         {ok, awesome}
    %% end.


