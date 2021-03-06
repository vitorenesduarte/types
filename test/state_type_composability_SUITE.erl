%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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
%%

-module(state_type_composability_SUITE).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([nowarn_export_all, export_all]).

-include("state_type.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.
init_per_testcase(_Case, Config) -> Config.
end_per_testcase(_Case, Config) -> Config.

all() ->
    [
        awmap_nested_rmv_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

awmap_nested_rmv_test(_Config) ->
    Actor = "A",
    CType = {?AWMAP_TYPE, [?AWSET_TYPE]},
    Map0 = ?AWMAP_TYPE:new([CType]),
    {ok, Map1} = ?AWMAP_TYPE:mutate({apply, "hello", {apply, "world_one", {add, 3}}}, Actor, Map0),
    {ok, Map2} = ?AWMAP_TYPE:mutate({apply, "hello", {apply, "world_two", {add, 7}}}, Actor, Map1),
    {ok, Map3} = ?AWMAP_TYPE:mutate({apply, "world", {apply, "hello", {add, 17}}}, Actor, Map2),
    {ok, Map4} = ?AWMAP_TYPE:mutate({apply, "hello", {rmv, "world_one"}}, Actor, Map3),
    {ok, Map5} = ?AWMAP_TYPE:mutate({rmv, "world"}, Actor, Map4),
    {ok, Map6} = ?AWMAP_TYPE:mutate({apply, "hello", {apply, "world_z", {add, 23}}}, Actor, Map5),
    {ok, Map7} = ?AWMAP_TYPE:mutate({apply, "hello", {rmv, "world_two"}}, Actor, Map6),
    {ok, Map8} = ?AWMAP_TYPE:mutate({apply, "hello", {rmv, "world_z"}}, Actor, Map7),
    Query3 = ?AWMAP_TYPE:query(Map3),
    Query4 = ?AWMAP_TYPE:query(Map4),
    Query5 = ?AWMAP_TYPE:query(Map5),
    Query6 = ?AWMAP_TYPE:query(Map6),
    Query7 = ?AWMAP_TYPE:query(Map7),
    Query8 = ?AWMAP_TYPE:query(Map8),

    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}, {"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query3),
    ?assertEqual([{"hello", [{"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query4),
    ?assertEqual([{"hello", [{"world_two", sets:from_list([7])}]}], Query5),
    ?assertEqual([{"hello", [{"world_two", sets:from_list([7])}, {"world_z", sets:from_list([23])}]}], Query6),
    ?assertEqual([{"hello", [{"world_z", sets:from_list([23])}]}], Query7),
    ?assertEqual([], Query8).

