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

%% @doc LWW Map.

-module(state_lwwmap).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2, delta_and_merge/2]).
-export([query/1, query/2, equal/2, is_bottom/1,
         is_inflation/2, is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/2, digest/1]).
-export([encode/2, decode/2]).

-export_type([state_lwwmap/0, state_lwwmap_op/0]).

-opaque state_lwwmap() :: {?TYPE, payload()}.
-type payload() :: maps:maps().
-type key() :: term().
-type timestamp() :: non_neg_integer().
-type value() :: term().
-type state_lwwmap_op() :: {set, key(), timestamp(), value()} | [{set, key(), timestamp(), value()}].

%% @doc Create a new, empty `state_lwwmap()'
-spec new() -> state_lwwmap().
new() ->
    {?TYPE, maps:new()}.

%% @doc Create a new, empty `state_lwwmap()'
-spec new([term()]) -> state_lwwmap().
new([]) ->
    new().

%% @doc Mutate a `state_lwwmap()'.
-spec mutate(state_lwwmap_op(), type:id(), state_lwwmap()) ->
    {ok, state_lwwmap()}.
mutate(Op, Actor, {?TYPE, _LWWMap}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_lwwmap()'.
-spec delta_mutate(state_lwwmap_op(), type:id(), state_lwwmap()) ->
    {ok, state_lwwmap()}.
delta_mutate({set, Key, Timestamp, Value}, _Actor, {?TYPE, _LWWMap}) ->
    Delta = maps:put(Key, {Timestamp, Value}, #{}),
    {ok, {?TYPE, Delta}};
delta_mutate(OpList, Actor, CRDT) ->
    Result = lists:foldl(
        fun(Op, Acc) ->
            %% NOTE: all operations are done on the original CRDT
            {ok, Delta} = delta_mutate(Op, Actor, CRDT),
            merge(Delta, Acc)
        end,
        new(),
        OpList
    ),
    {ok, Result}.

%% @doc Returns the value of the `state_lwwmap()'.
-spec query(state_lwwmap()) -> non_neg_integer().
query({?TYPE, LWWMap}) ->
    %% simply hide timestamps
    maps:map(fun(_, {_, V}) -> V end, LWWMap).

%% @doc Returns the value of the `state_lwwmap()', given a list
%%      of extra arguments.
-spec query(list(term()), state_lwwmap()) -> non_neg_integer().
query([MoreRecent], {?TYPE, LWWMap}) ->
    Keys = lists:reverse(lists:sort(maps:keys(LWWMap))),
    TopKeys = lists:sublist(Keys, MoreRecent),
    LWWMapTop = maps:with(TopKeys, LWWMap),
    query({?TYPE, LWWMapTop}).

%% @doc Merge two `state_lwwmap()'.
-spec merge(state_lwwmap(), state_lwwmap()) -> state_lwwmap().
merge({?TYPE, LWWMap1}, {?TYPE, LWWMap2}) ->
    LWWMap = maps_ext:merge_all(
        fun(_, {TS1, _}=V1, {TS2, _}=V2) ->
            case TS1 > TS2 of
                true -> V1;
                false -> V2
            end
        end,
        LWWMap1,
        LWWMap2
    ),
    {?TYPE, LWWMap}.

%% @doc Merge two LWWMap and return the delta responsible for the inflation.
-spec delta_and_merge(state_lwwmap(), state_lwwmap()) -> {state_lwwmap(), state_lwwmap()}.
delta_and_merge({?TYPE, Remote}, {?TYPE, Local}) ->
    {Delta, CRDT} = maps:fold(
        fun(RKey, {RTS, _}=RValue, {DeltaAcc, CRDTAcc}=Acc) ->
            %% inflation: when key is there with smaller ts
            %% and when it's not
            Inflation = case maps:find(RKey, CRDTAcc) of
                {ok, {LTS, _}} -> RTS > LTS;
                error -> true
            end,

            case Inflation of
                true -> {maps:put(RKey, RValue, DeltaAcc), maps:put(RKey, RValue, CRDTAcc)};
                false -> Acc
            end
        end,
        {#{}, Local},
        Remote
    ),
    {{?TYPE, Delta}, {?TYPE, CRDT}}.

%% @doc Are two `state_lwwmap()' equal?
-spec equal(state_lwwmap(), state_lwwmap()) -> boolean().
equal(_, _) ->
    undefined.

%% @doc Check if a LWWMap is bottom.
-spec is_bottom(state_lwwmap()) -> boolean().
is_bottom({?TYPE, LWWMap}) ->
    maps:size(LWWMap) == 0.

%% @doc Given two `state_lwwmap()', check if the second is an inflation
-spec is_inflation(state_lwwmap(), state_lwwmap()) -> boolean().
is_inflation(_, _) ->
    undefined.

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_lwwmap(), state_lwwmap()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_lwwmap(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation(_, _) ->
    undefined.

-spec digest(state_lwwmap()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_lwwmap()'.
-spec join_decomposition(state_lwwmap()) -> [state_lwwmap()].
join_decomposition(_) ->
    undefined.

%% @doc Delta calculation for `state_lwwmap()'.
-spec delta(state_lwwmap(),
            state_type:digest()) -> state_lwwmap().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_lwwmap()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_lwwmap().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


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

query_args_test() ->
    Map1 = {?TYPE, maps:from_list([{1, {10, 1}}, {2, {11, 13}}, {3, {12, 1}}])},
    ?assertEqual(maps:from_list([{1, 1}, {2, 13}, {3, 1}]), query([10], Map1)),
    ?assertEqual(maps:from_list([{1, 1}]), query([1], Map1)),
    ?assertEqual(maps:from_list([]), query([0], Map1)).

delta_set_test() ->
    Map0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({set, a, 10, v1}, 1, Map0),
    Map1 = merge({?TYPE, Delta1}, Map0),
    {ok, {?TYPE, Delta2}} = delta_mutate({set, a, 11, v2}, 2, Map1),
    Map2 = merge({?TYPE, Delta2}, Map1),
    {ok, {?TYPE, Delta3}} = delta_mutate({set, b, 12, v3}, 1, Map2),
    Map3 = merge({?TYPE, Delta3}, Map2),
    ?assertEqual({?TYPE, maps:from_list([{a, {10, v1}}])}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, maps:from_list([{a, {10, v1}}])}, Map1),
    ?assertEqual({?TYPE, maps:from_list([{a, {11, v2}}])}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, maps:from_list([{a, {11, v2}}])}, Map2),
    ?assertEqual({?TYPE, maps:from_list([{b, {12, v3}}])}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, maps:from_list([{a, {11, v2}}, {b, {12, v3}}])}, Map3).

delta_multiple_set_test() ->
    OpList = [{set, a, 10, v1}, {set, a, 11, v2}, {set, b, 12, v3}],
    {ok, {?TYPE, Delta}} = delta_mutate(OpList, 1, new()),
    ?assertEqual({?TYPE, maps:from_list([{a, {11, v2}}, {b, {12, v3}}])}, {?TYPE, Delta}).

merge_test() ->
    Map1 = {?TYPE, maps:from_list([{a, {10, v1}}, {b, {11, v2}}])},
    Map2 = {?TYPE, maps:from_list([{a, {12, v3}}, {c, {13, v4}}])},
    Expected = {?TYPE, maps:from_list([{a, {12, v3}}, {b, {11, v2}}, {c, {13, v4}}])},
    Map3 = merge(Map1, Map2),
    Map4 = merge(Map2, Map1),
    Map5 = merge(Map1, Map1),
    ?assertEqual(Expected, Map3),
    ?assertEqual(Expected, Map4),
    ?assertEqual(Map1, Map5).

delta_and_merge_test() ->
    Local1 = {?TYPE, maps:from_list([{a, {10, v1}}, {b, {11, v2}}])},
    Remote1 = {?TYPE, maps:from_list([{a, {12, v3}}, {c, {13, v4}}])},
    Remote2 = {?TYPE, maps:from_list([{a, {10, v1}}, {b, {14, v5}}])},
    {Delta1, Local2} = delta_and_merge(Remote1, Local1),
    %% merging again will return nothing new
    {Bottom, Local2} = delta_and_merge(Remote1, Local2),
    {Delta2, Local3} = delta_and_merge(Remote2, Local2),
    ?assertEqual(Remote1, Delta1),
    ?assert(is_bottom(Bottom)),
    ?assertEqual({?TYPE, maps:from_list([{a, {12, v3}}, {b, {11, v2}}, {c, {13, v4}}])}, Local2),
    ?assertEqual({?TYPE, maps:from_list([{b, {14, v5}}])}, Delta2),
    ?assertEqual({?TYPE, maps:from_list([{a, {12, v3}}, {b, {14, v5}}, {c, {13, v4}}])}, Local3).

is_bottom_test() ->
    Map0 = new(),
    Map1 = {?TYPE, maps:from_list([{a, {12, v3}}, {c, {13, v4}}])},
    ?assert(is_bottom(Map0)),
    ?assertNot(is_bottom(Map1)).

encode_decode_test() ->
    Map = {?TYPE, maps:from_list([{a, {12, v3}}, {c, {13, v4}}])},
    Binary = encode(erlang, Map),
    EMap = decode(erlang, Binary),
    ?assertEqual(Map, EMap).

-endif.
