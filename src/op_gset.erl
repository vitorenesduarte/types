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

%% @doc Op-based GSet CRDT.

-module(op_gset).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-behaviour(type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3]).
-export([query/1, equal/2]).

-export_type([op_gset/0, op_gset_op/0]).

-opaque op_gset() :: {?TYPE, payload()}.
-type element() :: term().
-type payload() :: sets:set(element()).
-type op_gset_op() :: {add, element()}.

%% @doc Create a new, empty `op_gset()'
-spec new() -> op_gset().
new() ->
    {?TYPE, sets:new()}.

%% @doc Create a new, empty `op_gset()'
-spec new([term()]) -> op_gset().
new([]) ->
    new().

%% @doc Mutate a `op_gset()'.
-spec mutate(op_gset_op(), type:id(), op_gset()) ->
    {ok, op_gset()}.
mutate({add, Element}, _, {?TYPE, Set}) ->
    {ok, {?TYPE, sets:add_element(Element, Set)}}.

%% @doc Delta-mutate a `op_gset()'.
-spec query(op_gset()) -> sets:set(element()).
query({?TYPE, Set}) ->
    Set.

%% @doc Are two `op_gset()'s structurally equal?
-spec equal(op_gset(), op_gset()) -> boolean().
equal({?TYPE, Set1}, {?TYPE, Set2}) ->
    sets_ext:equal(Set1, Set2).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, sets:new()}, new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, sets:from_list([a])},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([a]), query(Set1)).

add_test() ->
    Set0 = new(),
    {ok, Set1} = mutate({add, a}, z, Set0),
    {ok, Set2} = mutate({add, b}, z, Set1),
    {ok, Set3} = mutate({add, a}, z, Set2),
    ?assertEqual({?TYPE, sets:from_list([a])}, Set1),
    ?assertEqual({?TYPE, sets:from_list([a, b])}, Set2),
    ?assertEqual({?TYPE, sets:from_list([a, b])}, Set3).

equal_test() ->
    Set1 = {?TYPE, sets:from_list([a])},
    Set2 = {?TYPE, sets:from_list([b])},
    ?assert(equal(Set1, Set1)),
    ?assertNot(equal(Set1, Set2)).

-endif.
