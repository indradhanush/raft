-module(raft_tests).
-include_lib("eunit/include/eunit.hrl").


get_timeout_options_test_() ->
    [?_assertEqual(raft:get_timeout_options(10), {timeout, 3010, ticker})].
