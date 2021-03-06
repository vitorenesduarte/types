%%
%% Copyright (c) 2015-2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(state_type).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-include("state_type.hrl").

-export([new/1,
         mutate/3,
         is_inflation/2,
         is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([delta/2]).
-export([extract_args/1]).
-export([crdt_size/1,
         op_size/1,
         term_size/1,
         term_byte_size/1,
         digest_size/1]).

-export_type([state_type/0,
              crdt/0,
              digest/0,
              format/0,
              delta_method/0]).

%% Define some initial types.
-type state_type() :: state_awmap |
                      state_awset |
                      state_awset_ps |
                      state_boolean |
                      state_dwflag |
                      state_ewflag |
                      state_gcounter |
                      state_gset |
                      state_ivar |
                      state_lexcounter |
                      state_lwwmap |
                      state_lwwregister |
                      state_max_int |
                      state_mvregister |
                      state_mvmap |
                      state_orset |
                      state_pncounter |
                      state_twopset.
-type crdt() :: {state_type(), type:payload()}.
-type digest() :: {state, crdt()} | %% state as digest
                  {mdata, term()}.  %% metadata as digest
-type delta_method() :: state | mdata.

%% Supported serialization formats.
-type format() :: erlang.

%% Perform a delta mutation.
-callback delta_mutate(type:operation(), type:id(), crdt()) ->
    {ok, crdt()} | {error, type:error()}.

%% Merge two replicas.
%% If we merge two CRDTs, the result is a CRDT.
%% If we merge a delta and a CRDT, the result is a CRDT.
%% If we merge two deltas, the result is a delta (delta group).
-callback merge(crdt(), crdt()) -> crdt().

%% Check if a some state is bottom
-callback is_bottom(crdt()) -> boolean().

%% Inflation testing.
-callback is_inflation(crdt(), crdt()) -> boolean().
-callback is_strict_inflation(crdt(), crdt()) -> boolean().

%% Let A be the first argument.
%% Let B be the second argument.
%% A is a join-irreducible state.
%% This functions checks if A will strictly inflate B.
%% B can be a CRDT or a digest of a CRDT.
-callback irreducible_is_strict_inflation(crdt(),
                                          digest()) -> boolean().

%% CRDT digest (which can be the CRDT state itself).
-callback digest(crdt()) -> digest().

%% Join decomposition.
-callback join_decomposition(crdt()) -> [crdt()].

%% Let A be the first argument.
%% Let B be the second argument.
%% This function returns a ∆ from A that inflates B.
%% "The join of all s in join_decomposition(A) such that s strictly
%% inflates B"
-callback delta(crdt(), digest()) -> crdt().

%% @todo These should be moved to type.erl
%% Encode and Decode.
-callback encode(format(), crdt()) -> binary().
-callback decode(format(), binary()) -> crdt().

%% @doc Builds a new CRDT from a given CRDT
-spec new(crdt()) -> any(). %% @todo Fix this any()
new({Type, _Payload}) ->
    Type:new().

%% @doc Generic Join composition.
-spec mutate(type:operation(), type:id(), crdt()) ->
    {ok, crdt()} | {error, type:error()}.
mutate(Op, Actor, {Type, _}=CRDT) ->
    case Type:delta_mutate(Op, Actor, CRDT) of
        {ok, {Type, Delta}} ->
            {ok, Type:merge({Type, Delta}, CRDT)};
        Error ->
            Error
    end.

%% @doc Generic check for inflation.
-spec is_inflation(crdt(), crdt()) -> boolean().
is_inflation({Type, _}=CRDT1, {Type, _}=CRDT2) ->
    Type:equal(Type:merge(CRDT1, CRDT2), CRDT2).

%% @doc Generic check for strict inflation.
%%      We have a strict inflation if:
%%          - we have an inflation
%%          - we have different CRDTs
-spec is_strict_inflation(crdt(), crdt()) -> boolean().
is_strict_inflation({Type, _}=CRDT1, {Type, _}=CRDT2) ->
    Type:is_inflation(CRDT1, CRDT2) andalso
    not Type:equal(CRDT1, CRDT2).

%% @doc Generic check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(crdt(),
                                      digest()) -> boolean().
irreducible_is_strict_inflation({Type, _}=Irreducible,
                                {state, {Type, _}=CRDT}) ->
    Merged = Type:merge(Irreducible, CRDT),
    Type:is_strict_inflation(CRDT, Merged).

%% @doc Generic delta calculation.
-spec delta({decomposition, [crdt()]} | crdt(), digest()) -> crdt().
delta({decomposition, Decomp}, B) ->
    do_delta(Decomp, B);
delta({Type, _}=A, B) ->
    Decomp = Type:join_decomposition(A),
    case Decomp of
        [] -> new(A);
        _ -> do_delta(Decomp, B)
    end.

%% @private
do_delta([{Type, _}=H|_]=Decomp, B) ->
    lists:foldl(
        fun(Irreducible, Acc) ->
            case Type:irreducible_is_strict_inflation(Irreducible,
                                                      B) of
                true ->
                    Type:merge(Irreducible, Acc);
                false ->
                    Acc
            end
        end,
        new(H),
        Decomp
    ).

%% @doc extract arguments from complex (composite) types
extract_args({Type, Args}) ->
    {Type, Args};
extract_args(Type) ->
    {Type, []}.

%% @doc CRDT size.
%%      First component is the metadata size,
%%      the second component is the payload size.
-spec crdt_size(crdt()) -> non_neg_integer().
crdt_size({?GCOUNTER_TYPE, CRDT}) ->
    maps:size(CRDT);
crdt_size({?GSET_TYPE, CRDT}) ->
    sets:size(CRDT);
crdt_size({?LWWMAP_TYPE, CRDT}) ->
    maps:size(CRDT);
crdt_size({op_gcounter, _}) ->
    1;
crdt_size({op_gset, CRDT}) ->
    sets:size(CRDT);
crdt_size({op_lwwmap, CRDT}) ->
    maps:size(CRDT).

%% @doc Compute the size of an operation.
-spec op_size(term()) -> non_neg_integer().
op_size(increment) -> 1;
op_size({add, _}) -> 1;
%% the following considers a key-value pair as 1
%% which is what we're doing when saying the size of a lwwmap
%% is the size of the internal map
op_size({set, _, _, _}) -> 1;
op_size([H|T]) -> op_size(H) + op_size(T);
op_size([]) -> 0.


-spec term_size(crdt()) -> non_neg_integer().
term_size({?GSET_TYPE, CRDT}) ->
    sets:fold(
        fun(Element, Acc) ->  Acc + term_byte_size(Element) end,
        0,
        CRDT
    );
term_size({?LWWMAP_TYPE, CRDT}) ->
    maps:fold(
        fun(Key, {_Timestamp, Value}, Acc) ->
            Acc + term_byte_size(Key) + term_byte_size(Value)
        end,
        0,
        CRDT
    ).

-spec term_byte_size(term()) -> non_neg_integer().
term_byte_size(Term) when is_binary(Term) ->
    erlang:byte_size(Term);
term_byte_size(Term) ->
    erlang:byte_size(erlang:term_to_binary(Term)).

%% @doc Digest size.
digest_size({ActiveDots, CausalContext}) ->
    %% awset
    dot_set_size(ActiveDots) +
    causal_context_size(CausalContext).

%% @private
dot_set_size(DotSet) ->
    ordsets:size(DotSet).

%% @private
causal_context_size({Compressed, DotSet}) ->
    %% size of the causal context is the number of entries
    %% in the compressed component plus the number
    %% of the dots in the non-compressed part
    orddict:size(Compressed) + dot_set_size(DotSet).
