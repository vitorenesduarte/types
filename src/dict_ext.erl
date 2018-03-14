%%
%% Copyright (c) 2018 Christopher Meiklejohn.  All Rights Reserved.
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

-module(dict_ext).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-export([equal/3,
         fetch/3]).

-spec equal(dict:dict(), dict:dict(), function()) ->
    boolean().
equal(Dict1, Dict2, Fun) ->
    case dict:size(Dict1) == dict:size(Dict2) of
        true ->
            %% check all entries are the same
            do_equal(dict:to_list(Dict1), Dict2, Fun);
        false ->
            false
    end.

do_equal([], _Dict2, _Fun) ->
    true;
do_equal([{K, V1}|T], Dict2, Fun) ->
    case dict:find(K, Dict2) of
        {ok, V2} ->
            case Fun(V1, V2) of
                true -> do_equal(T, Dict2, Fun);
                false -> false
            end;
        error ->
            false
    end.

%% @doc
fetch(K, M, Default) ->
    case dict:find(K, M) of
        {ok, V} ->
            V;
        error ->
            Default
    end.
