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

-module(prop_join_decompositions).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("state_type.hrl").

%% common
-define(ACTOR, oneof([a, b, c])).
-define(P(T, Actor), {T, Actor}).
-define(PA(T), ?P(T, a)).
-define(PB(T), ?P(T, b)).
-define(L(T), list(?P(T, ?ACTOR))).

%% primitives
-define(TRUE, true).

%% counters
-define(INC, increment).
-define(DEC, decrement).
-define(INCDEC, oneof([?INC, ?DEC])).

%% sets
-define(ELEMENT, oneof([1, 2, 3])).
-define(ADD, {add, ?ELEMENT}).
-define(RMV, {rmv, ?ELEMENT}).
-define(ADDRMV, oneof([?ADD, ?RMV])).

%% registers
-define(TIMESTAMP, non_neg_integer()).
-define(SET, {set, ?TIMESTAMP, ?ELEMENT}).


%% primitives
prop_boolean_decomposition() ->
    ?FORALL(L, ?L(?TRUE),
            check_decomposition(create(?BOOLEAN_TYPE, L))).
prop_boolean_redundant() ->
    ?FORALL(L, ?L(?TRUE),
            check_redundant(create(?BOOLEAN_TYPE, L))).
prop_boolean_irreducible() ->
    ?FORALL({L, A, B}, {?L(?TRUE), ?PA(?TRUE), ?PB(?TRUE)},
            check_irreducible(create(?BOOLEAN_TYPE, L),
                              create(?BOOLEAN_TYPE, [A]),
                              create(?BOOLEAN_TYPE, [B]))
    ).

prop_max_int_decomposition() ->
    ?FORALL(L, ?L(?INC),
            check_decomposition(create(?MAX_INT_TYPE, L))).
prop_max_int_redundant() ->
    ?FORALL(L, ?L(?INC),
            check_redundant(create(?MAX_INT_TYPE, L))).
prop_max_int_irreducible() ->
    ?FORALL({L, A, B}, {?L(?INC), ?PA(?INC), ?PB(?INC)},
            check_irreducible(create(?MAX_INT_TYPE, L),
                              create(?MAX_INT_TYPE, [A]),
                              create(?MAX_INT_TYPE, [B]))
    ).


%% %% counters
%% prop_gcounter_decomposition() ->
%%     ?FORALL(L, ?L(?INC),
%%             check_decomposition(create(?GCOUNTER_TYPE, L))).
%% prop_gcounter_redundant() ->
%%     ?FORALL(L, ?L(?INC),
%%             check_redundant(create(?GCOUNTER_TYPE, L))).
%% prop_gcounter_irreducible() ->
%%     ?FORALL({L, A, B}, {?L(?INC), ?PA(?INC), ?PB(?INC)},
%%             check_irreducible(create(?GCOUNTER_TYPE, L),
%%                               create(?GCOUNTER_TYPE, [A]),
%%                               create(?GCOUNTER_TYPE, [B]))
%%     ).

prop_pncounter_decomposition() ->
    ?FORALL(L, ?L(?INCDEC),
            check_decomposition(create(?PNCOUNTER_TYPE, L))).
prop_pncounter_redundant() ->
    ?FORALL(L, ?L(?INCDEC),
            check_redundant(create(?PNCOUNTER_TYPE, L))).
prop_pncounter_irreducible() ->
    ?FORALL({L, A, B}, {?L(?INCDEC), ?PA(?INCDEC), ?PB(?INCDEC)},
            check_irreducible(create(?PNCOUNTER_TYPE, L),
                              create(?PNCOUNTER_TYPE, [A]),
                              create(?PNCOUNTER_TYPE, [B]))
    ).

%% sets
prop_gset_decomposition() ->
    ?FORALL(L, ?L(?ADD),
            check_decomposition(create(?GSET_TYPE, L))).
prop_gset_redundant() ->
    ?FORALL(L, ?L(?ADD),
            check_redundant(create(?GSET_TYPE, L))).
prop_gset_irreducible() ->
    ?FORALL({L, A, B}, {?L(?ADD), ?PA(?ADD), ?PB(?ADD)},
            check_irreducible(create(?GSET_TYPE, L),
                              create(?GSET_TYPE, [A]),
                              create(?GSET_TYPE, [B]))
    ).
prop_gset_digest() ->
    ?FORALL({L1, L2}, {?L(?ADD), ?L(?ADD)},
            check_digest(create(?GSET_TYPE, L1),
                         create(?GSET_TYPE, L2))).

prop_twopset_decomposition() ->
    ?FORALL(L, ?L(?ADDRMV),
            check_decomposition(create(?TWOPSET_TYPE, L))).
prop_twopset_redundant() ->
    ?FORALL(L, ?L(?ADDRMV),
            check_redundant(create(?TWOPSET_TYPE, L))).
prop_twopset_irreducible() ->
    ?FORALL({L, A, B}, {?L(?ADDRMV), ?PA(?ADDRMV), ?PB(?ADDRMV)},
            check_irreducible(create(?TWOPSET_TYPE, L),
                              create(?TWOPSET_TYPE, [A]),
                              create(?TWOPSET_TYPE, [B]))
    ).

prop_awset_decomposition() ->
    ?FORALL(L, ?L(?ADDRMV),
            check_decomposition(create(?AWSET_TYPE, L))).
prop_awset_redundant() ->
    ?FORALL(L, ?L(?ADDRMV),
            check_redundant(create(?AWSET_TYPE, L))).
prop_awset_irreducible() ->
    ?FORALL({L, A, B}, {?L(?ADDRMV), ?PA(?ADDRMV), ?PB(?ADDRMV)},
            check_irreducible(create(?AWSET_TYPE, L),
                              create(?AWSET_TYPE, [A]),
                              create(?AWSET_TYPE, [B]))
    ).
prop_awset_digest() ->
    ?FORALL({L1, L2}, {?L(?ADDRMV), ?L(?ADDRMV)},
            check_digest(create(?AWSET_TYPE, L1),
                         create(?AWSET_TYPE, L2))).

prop_orset_decomposition() ->
    ?FORALL(L, ?L(?ADDRMV),
            check_decomposition(create(?ORSET_TYPE, L))).
prop_orset_redundant() ->
    ?FORALL(L, ?L(?ADDRMV),
            check_redundant(create(?ORSET_TYPE, L))).
prop_orset_irreducible() ->
    ?FORALL({L, A, B}, {?L(?ADDRMV), ?PA(?ADDRMV), ?PB(?ADDRMV)},
            check_irreducible(create(?ORSET_TYPE, L),
                              create(?ORSET_TYPE, [A]),
                              create(?ORSET_TYPE, [B]))
    ).

%% registers
prop_lwwregister_decomposition() ->
    ?FORALL(L, ?L(?SET),
            check_decomposition(create(?LWWREGISTER_TYPE, L))).
prop_lwwregister_redundant() ->
    ?FORALL(L, ?L(?SET),
            check_redundant(create(?LWWREGISTER_TYPE, L))).
prop_lwwregister_irreducible() ->
    ?FORALL({L, A, B}, {?L(?SET), ?PA(?SET), ?PB(?SET)},
            check_irreducible(create(?LWWREGISTER_TYPE, L),
                              create(?LWWREGISTER_TYPE, [A]),
                              create(?LWWREGISTER_TYPE, [B]))
    ).

prop_mvregister_decomposition() ->
    ?FORALL(L, ?L(?SET),
            check_decomposition(create(?MVREGISTER_TYPE, L))).
prop_mvregister_redundant() ->
    ?FORALL(L, ?L(?SET),
            check_redundant(create(?MVREGISTER_TYPE, L))).
prop_mvregister_irreducible() ->
    %% @todo
    true.


%% @private
check_decomposition({Type, _}=CRDT) ->
    Bottom = state_type:new(CRDT),

    %% the join of the decomposition should given the CRDT
    JD = Type:join_decomposition(CRDT),
    Merged = merge_all(Bottom, JD),
    Type:equal(CRDT, Merged).

%% @private
check_redundant({Type, _}=CRDT) ->
    Bottom = state_type:new(CRDT),

    ?IMPLIES(
        not Type:is_bottom(CRDT),
        begin
            JD = Type:join_decomposition(CRDT),

            %% if we remove one element from the decomposition,
            %% the rest is not enough to produce the CRDT
            Random = rand:uniform(length(JD)),
            Element = lists:nth(Random, JD),
            Rest = merge_all(Bottom, JD -- [Element]),
            Type:is_strict_inflation(Rest, CRDT)
       end
    ).

%% @private
check_irreducible({Type, _}=CRDT, A, B) ->
    Merged = Type:merge(A, B),
    Tests = lists:map(
        fun(Irreducible) ->

            %% check that all the elements in the join decomposition
            %% are join-irreducible
            Test = ?IMPLIES(
                Type:equal(Merged, Irreducible),
                Type:equal(A, Irreducible) orelse
                Type:equal(B, Irreducible)
            ),

            {Irreducible, Test}
        end,
        Type:join_decomposition(CRDT)
    ),

    conjunction(Tests).

%% @private
check_digest({Type, _}=CRDT1, {Type, _}=CRDT2) ->

    %% the delta of two states,
    %% using the original state
    %% or a digest of that state,
    %% should be the same delta
    DeltaState = Type:delta(CRDT1, {state, CRDT2}),
    DeltaDigest = Type:delta(CRDT1, Type:digest(CRDT2)),

    Type:equal(DeltaState, DeltaDigest).

%% @private
merge_all({Type, _}=Bottom, CRDTList) ->
    lists:foldl(
        fun(CRDT, Acc) ->
            Type:merge(CRDT, Acc)
        end,
        Bottom,
        CRDTList
    ).

%% @private
create(Type, L) ->
    %% get list of actors
    Actors = lists:usort([Actor || {_Op, Actor} <- L]),

    %% create an dictionary from actors to list of ops
    OpsPerActor = lists:foldl(
        fun(Actor, Acc) ->
            Ops = lists:filtermap(
                fun({Op, OpActor}) ->
                    case Actor == OpActor of
                        true ->
                            {true, Op};
                        false ->
                            false
                    end
                end,
                L
            ),
            orddict:store(Actor, Ops, Acc)
        end,
        orddict:new(),
        Actors
    ),

    %% create a CRDT per actor, by applying its list of ops
    CRDTList = orddict:fold(
        fun(Actor, Ops, Acc) ->
            CRDT = lists:foldl(
                fun(Op, CRDT0) ->
                    {ok, CRDT1} = Type:mutate(Op, Actor, CRDT0),
                    CRDT1
                end,
                Type:new(),
                Ops
            ),

            [CRDT | Acc]
        end,
        [],
        OpsPerActor
    ),

    %% merge the list of CRDTs
    merge_all(Type:new(), CRDTList).
