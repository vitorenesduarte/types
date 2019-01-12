%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Op-based GCounter CRDT.

-module(op_gcounter).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-behaviour(type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3]).
-export([query/1, equal/2]).

-export_type([op_gcounter/0, op_gcounter_op/0]).

-opaque op_gcounter() :: {?TYPE, payload()}.
-type payload() :: non_neg_integer().
-type op_gcounter_op() :: increment.

%% @doc Create a new, empty `op_gcounter()'
-spec new() -> op_gcounter().
new() ->
    {?TYPE, 0}.

%% @doc Create a new, empty `op_gcounter()'
-spec new([term()]) -> op_gcounter().
new([]) ->
    new().

%% @doc Mutate a `op_gcounter()'.
-spec mutate(op_gcounter_op(), type:id(), op_gcounter()) ->
    {ok, op_gcounter()}.
mutate(increment, _, {?TYPE, Counter}) ->
    {ok, {?TYPE, Counter + 1}}.

%% @doc Delta-mutate a `op_gcounter()'.
-spec query(op_gcounter()) -> non_neg_integer().
query({?TYPE, Counter}) ->
    Counter.

%% @doc Are two `op_gcounter()'s structurally equal?
-spec equal(op_gcounter(), op_gcounter()) -> boolean().
equal({?TYPE, Counter1}, {?TYPE, Counter2}) ->
    Counter1 == Counter2.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, 0}, new()).

query_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, 15},
    ?assertEqual(0, query(Counter0)),
    ?assertEqual(15, query(Counter1)).

increment_test() ->
    Counter0 = new(),
    {ok, Counter1} = mutate(increment, a, Counter0),
    {ok, Counter2} = mutate(increment, b, Counter1),
    ?assertEqual({?TYPE, 1}, Counter1),
    ?assertEqual({?TYPE, 2}, Counter2).

equal_test() ->
    Counter1 = {?TYPE, 1},
    Counter2 = {?TYPE, 2},
    ?assert(equal(Counter1, Counter1)),
    ?assertNot(equal(Counter1, Counter2)).

-endif.
