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

-module(maps_ext).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-export([merge_all/3]).

%% @doc
merge_all(MergeFun, MapA, MapB) ->
    %% merge A and with B
    %% (what's in B that's not in A won't be in `Map0')
    Map0 = maps:map(
        fun(Key, ValueA) ->
            case maps:find(Key, MapB) of
                {ok, ValueB} -> MergeFun(Key, ValueA, ValueB);
                error -> ValueA
            end
        end,
        MapA
    ),
    %% merge B with `Map0'
    maps:merge(MapB, Map0).
