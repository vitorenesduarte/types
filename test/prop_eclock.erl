%% -------------------------------------------------------------------
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
%%

-module(prop_eclock).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ACTOR, oneof([a, b, c])).
-define(DOT, {?ACTOR, dot_store:dot_sequence()}).
-define(DOTL, list(?DOT)).

prop_from_dots() ->
    ?FORALL(
       L,
       ?DOTL,
       begin
            %% if we construct a cc from a list of dots,
            %% all of those dots should be there
            CC = cc(L),
            lists:foldl(
                fun(Dot, Acc) ->
                    Acc andalso eclock:is_element(Dot, CC)
                end,
                true,
                L
            )
       end
    ).

prop_add_dot() ->
    ?FORALL(
        {Dot, L},
        {?DOT, ?DOTL},
        begin
            %% if we add a dot to a cc it should be there
            CC = cc(L),
            eclock:is_element(
                Dot,
                eclock:add_dot(Dot, CC)
            )
        end
    ).

prop_next_dot() ->
    ?FORALL(
        {Actor, L},
        {?ACTOR, ?DOTL},
        begin
            %% the next dot should not be part of the cc.
            CC = cc(L),
            Dot = eclock:next_dot(Actor, CC),
            not eclock:is_element(
                Dot,
                CC
            )
        end
    ).

prop_union() ->
    ?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            CC1 = cc(L1),
            CC2 = cc(L2),
            Union = eclock:union(CC1, CC2),

            %% Dots from the cc's belong to the union.
            R1 = dot_set:fold(
                fun(Dot, Acc) ->
                    Acc andalso
                    eclock:is_element(Dot, Union)
                end,
                true,
                dot_set:union(eclock:dots(CC1),
                              eclock:dots(CC2))
            ),

            %% Dots from the union belong to one of the cc's.
            R2 =  dot_set:fold(
                fun(Dot, Acc) ->
                    Acc andalso
                    (
                        eclock:is_element(Dot, CC1) orelse
                        eclock:is_element(Dot, CC2)
                    )
                end,
                true,
                eclock:dots(Union)
            ),

            R1 andalso R2
        end
    ).

%% @private
prop_subtract() ->
    ?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            C1 = cc(L1),
            C2 = cc(L2),
            IdToSequences = eclock:subtract(C1, C2),
            V1 = orddict:fold(
                fun(Id, Sequences, Acc0) ->
                    lists:foldl(
                        fun(Sn, Acc1) ->
                            dot_set:add_dot({Id, Sn}, Acc1)
                        end,
                        Acc0,
                        Sequences
                    )
                end,
                dot_set:new(),
                IdToSequences
            ),

            Set1 = eclock:dots(C1),
            Set2 = eclock:dots(C2),
            V2 = dot_set:subtract(Set1, Set2),

            V1 == V2
        end
    ).

%% @private
ds(L) ->
    dot_set:from_dots(L).

%% @private
cc(L) ->
    eclock:from_dot_set(ds(L)).
