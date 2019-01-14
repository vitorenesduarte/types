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

%% @doc Op-based LWWMap CRDT.

-module(op_lwwmap).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-behaviour(type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3]).
-export([query/1, equal/2]).

-export_type([op_lwwmap/0, op_lwwmap_op/0]).

-opaque op_lwwmap() :: {?TYPE, payload()}.
-type payload() :: maps:map(key(), {timestamp(), value()}).
-type key() :: term().
-type timestamp() :: non_neg_integer().
-type value() :: term().
-type op_lwwmap_op() :: {set, key(), timestamp(), value()} |
                        [{set, key(), timestamp(), value()}].

%% @doc Create a new, empty `op_lwwmap()'
-spec new() -> op_lwwmap().
new() ->
    {?TYPE, maps:new()}.

%% @doc Create a new, empty `op_lwwmap()'
-spec new([term()]) -> op_lwwmap().
new([]) ->
    new().

%% @doc Mutate a `op_lwwmap()'.
-spec mutate(op_lwwmap_op(), type:id(), op_lwwmap()) ->
    {ok, op_lwwmap()}.
mutate({set, Key, Timestamp, Value}, _, {?TYPE, Map0}) ->
    ShouldAdd = case maps:find(Key, Map0) of
        {ok, {CurrentTimestamp, _}} -> Timestamp > CurrentTimestamp;
        error -> true
    end,

    Map1 = case ShouldAdd of
        true -> maps:put(Key, {Timestamp, Value}, Map0);
        false -> Map0
    end,
    {ok, {?TYPE, Map1}};

mutate(OpList, Id, Map) ->
    Result = lists:foldl(
        fun(Op, MapAcc0) ->
            {ok, MapAcc1} = mutate(Op, Id, MapAcc0),
            MapAcc1
        end,
        Map,
        OpList
    ),
    {ok, Result}.

%% @doc Map-mutate a `op_lwwmap()'.
-spec query(op_lwwmap()) -> maps:map(key(), value()).
query({?TYPE, Map}) ->
    %% simply hide timestamps
    maps:map(fun(_, {_, V}) -> V end, Map).

%% @doc Are two `op_lwwmap()'s structurally equal?
-spec equal(op_lwwmap(), op_lwwmap()) -> boolean().
equal(_, _) ->
    undefined.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, #{}}, new()).

query_test() ->
    Map0 = new(),
    Map1 = {?TYPE, maps:from_list([{1, {10, 1}}, {2, {11, 13}}, {3, {12, 1}}])},
    ?assertEqual(#{}, query(Map0)),
    ?assertEqual(maps:from_list([{1, 1}, {2, 13}, {3, 1}]), query(Map1)).

set_test() ->
    Map0 = new(),
    {ok, Map1} = mutate({set, a, 10, v1}, 1, Map0),
    {ok, Map2} = mutate({set, a, 11, v2}, 2, Map1),
    {ok, Map3} = mutate({set, b, 12, v3}, 1, Map2),
    ?assertEqual({?TYPE, maps:from_list([{a, {10, v1}}])}, Map1),
    ?assertEqual({?TYPE, maps:from_list([{a, {11, v2}}])}, Map2),
    ?assertEqual({?TYPE, maps:from_list([{a, {11, v2}}, {b, {12, v3}}])}, Map3).

multiple_set_test() ->
    OpList = [{set, a, 10, v1}, {set, a, 11, v2}, {set, b, 12, v3}],
    {ok, Map} = mutate(OpList, 1, new()),
    ?assertEqual({?TYPE, maps:from_list([{a, {11, v2}}, {b, {12, v3}}])}, Map).

-endif.
