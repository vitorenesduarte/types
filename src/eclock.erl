%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 IMDEA Software Institute.  All Rights Reserved.
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

%% @doc Exception Clock.

-module(eclock).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-export([new/0,
         from_dot_set/1,
         dots/1,
         next_dot/2,
         add_dot/2,
         is_element/2,
         union/2,
         exceptions/2,
         dot_count/1]).

-export_type([eclock/0]).

-define(BOTTOM, {0, []}).

%% exception clock
-type dot_set() :: dot_store:dot_set().
-type dot() :: dot_store:dot().
-type sn() :: dot_store:dot_sequence().
-type per_node() :: {sn(), ordsets:ordset(sn())}.
-type actor() :: dot_store:dot_actor().
-type eclock() :: orddict:orddict(actor(), per_node()).

%% @doc Create an empty Exception Clock.
-spec new() -> eclock().
new() ->
    orddict:new().

%% @doc Create an Exception Clock from a dot set.
-spec from_dot_set(dot_set()) -> eclock().
from_dot_set(DotSet) ->
    dot_set:fold(
        fun(Dot, EClock) ->
            eclock:add_dot(Dot, EClock)
        end,
        eclock:new(),
        DotSet
    ).

%% @doc Return a dot set, given an Exception Clock.
-spec dots(eclock()) -> dot_set().
dots(EClock) ->
    orddict:fold(
        fun(Id, {Sequence, Exceptions}, DotSet0) ->

            %% add all dots from 1 to Sequence
            %% that are not exceptions
            lists:foldl(
                fun(S, DotSet1) ->
                    Dot = {Id, S},
                    dot_set:add_dot(Dot, DotSet1)
                end,
                DotSet0,
                lists:seq(1, Sequence) -- Exceptions
            )
        end,
        dot_set:new(),
        EClock
    ).

%% @doc Generate the next dot.
-spec next_dot(actor(), eclock()) -> dot().
next_dot(Id, EClock0) ->
    %% retrieve current value and exceptions
    {Sequence, _Exceptions} = orddict_ext:fetch(Id, EClock0, ?BOTTOM),

    %% create next dot
    {Id, Sequence + 1}.

%% @doc Add a dot to the Exception Clock.
-spec add_dot(dot(), eclock()) -> eclock().
add_dot({Id, Sequence}, EClock) ->
    {CurrentValue, Exceptions} = orddict_ext:fetch(Id, EClock, ?BOTTOM),
    NextEntry = case Sequence >= CurrentValue + 1 of
        true ->
            %% add all possible exceptions
            NewExceptions = lists:foldl(
                fun(S, Acc) ->
                    ordsets:add_element(S, Acc)
                end,
                Exceptions,
                lists:seq(CurrentValue + 1, Sequence - 1)
            ),

            %% update vector entry
            %% and possible new exceptions
            {Sequence, NewExceptions};
        false ->

            %% otherwise remove from exceptions
            %% (in case it's there)
            NewExceptions = ordsets:del_element(Sequence,
                                                Exceptions),
            {CurrentValue, NewExceptions}
    end,

    orddict:store(Id, NextEntry, EClock).

%% @doc Check if a dot belongs to the Exception Clock.
-spec is_element(dot(), eclock()) -> boolean().
is_element({Id, Sequence}, EClock) ->
    {Current, Exceptions} = orddict_ext:fetch(Id, EClock, ?BOTTOM),
    Sequence =< Current andalso
    not ordsets:is_element(Sequence, Exceptions).

%% @doc Union of two Exception Clocks.
-spec union(eclock(), eclock()) -> eclock().
union(EClockA, EClockB) ->
    orddict:merge(
        fun(_Id, {SequenceA, ExceptionsA},
                 {SequenceB, ExceptionsB}) ->

            Sequence = max(SequenceA, SequenceB),

            %% it's still an exception if
            %% it is an exception in the other clock
            %% or if the other clock max entry is less
            %% than the sequence of this exception
            ExA = ordsets:filter(
                fun(S) ->
                    S > SequenceB orelse
                    ordsets:is_element(S, ExceptionsB)
                end,
                ExceptionsA
            ),

            ExB = ordsets:filter(
                fun(S) ->
                    S > SequenceA orelse
                    ordsets:is_element(S, ExceptionsA)
                end,
                ExceptionsB
            ),

            Exceptions = ordsets:union(ExA, ExB),

            {Sequence, Exceptions}
        end,
        EClockA,
        EClockB
    ).

%% @doc Get the exceptions of some node
%%      with id `Id'.
-spec exceptions(actor(), eclock()) -> list(sn()).
exceptions(Id, EClock) ->
    {_, Exceptions} = orddict_ext:fetch(Id, EClock, ?BOTTOM),
    Exceptions.

%% @doc Get the dot count in the EClock.
%%      - number of nodes in the clock
%%      plus total number of exceptions
-spec dot_count(eclock()) -> non_neg_integer().
dot_count(EClock) ->
    orddict:fold(
        fun({_, {_, Exceptions}}, Acc) ->
            Acc + 1 + ordsets:size(Exceptions)
        end,
        0,
        EClock
    ).
