%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_eleveldb_index).

-include("rabbit_msg_store.hrl").

-behaviour(rabbit_msg_store_index).

-export([new/1, recover/1,
         lookup/2, insert/2, update/2, update_fields/3, delete/2,
         delete_object/2, delete_by_file/2, terminate/1]).

-define(INDEX_DIR, "eleveldb").
-define(RECOVER_NO_FILE, "recover_no_file").

-record(state, { db, dir, read_options, write_options, recovery_index }).

new(BaseDir) ->
    % TODO: recover after crash
    Dir = filename:join(BaseDir, ?INDEX_DIR),
    rabbit_file:recursive_delete([Dir]),
    {ok, DbRef} = eleveldb:open(Dir, [{create_if_missing, true}, {write_buffer_size, 5242880}]),

    {ok, RecoverIndex} = init_recovery_index(BaseDir),
    new_state(DbRef, Dir, RecoverIndex).

init_recovery_index(BaseDir) ->
    RecoverNoFileDir = filename:join(BaseDir, ?RECOVER_NO_FILE),
    rabbit_file:recursive_delete([RecoverNoFileDir]),
    eleveldb:open(RecoverNoFileDir, [{create_if_missing, true}, {write_buffer_size, 5242880}]).

recover(BaseDir) ->
    {ok, RecoverIndex} = init_recovery_index(BaseDir),

    Dir = filename:join(BaseDir, ?INDEX_DIR),
    case eleveldb:open(Dir, [{create_if_missing, false}, {write_buffer_size, 5242880}]) of
        {ok, DbRef} -> {ok, new_state(DbRef, Dir, RecoverIndex)};
        Error       -> Error
    end.

new_state(DbRef, Dir, RecoverIndex) ->
     #state{ db = DbRef,
             dir = Dir,
             read_options = [],
             write_options = [],
             recovery_index = RecoverIndex }.

lookup(Key, State) ->
rabbit_log:error("Read index ~p~n", [{Key, State}]),
    case eleveldb:get(State#state.db, Key, State#state.read_options) of
        not_found    -> not_found;
        {ok, Val}    -> decode_val(Val);
        {error, Err} -> {error, Err}
    end.

insert(Obj, State) ->
% TODO: difference between insert and update
rabbit_log:error("Write index ~p~n", [{Obj, State}]),
    Key = Obj#msg_location.msg_id,
    maybe_update_index(Key, Obj, State),
    Val = encode_val(Obj),
    ok = eleveldb:put(State#state.db, Key, Val, State#state.write_options).

update(Obj, State) ->
    insert(Obj, State).

update_fields(Key, Updates, State) ->
% TODO: pass initial object to avoid read
    Old = lookup(Key, State),
    New = update_elements(Old, Updates),
    update(New, State).

delete(Key, State) ->
rabbit_log:error("Delete index ~p~n", [{Key, State}]),
    maybe_delete_index(Key, State),
    ok = eleveldb:delete(State#state.db, Key, State#state.write_options).

delete_object(Obj, State) ->
% TODO: remove this function
    Key = Obj#msg_location.msg_id,
    case lookup(Key, State) of
        Obj -> delete(Key, State);
        _   -> ok
    end.

delete_by_file(undefined, State) ->
% This function is called only with `undefined` file name
% TODO: refactor index recovery to avoid additional table
% TODO: refactor index to modify the state.
    eleveldb:fold_keys(
        State#state.recovery_index,
        fun(Key, nothing) ->
            delete(Key, State#state{recovery_index = undefined})
        end,
        nothing,
        State#state.read_options),
    clear_index(State),
    ok.

terminate(#state { db = DB, dir = Dir, recovery_index = RecoverIndex }) ->
    eleveldb:close(RecoverIndex),
    RecoverNoFileDir = filename:join(filename:dirname(Dir), ?RECOVER_NO_FILE),
    eleveldb:destroy(RecoverNoFileDir, [{write_buffer_size, 5242880}]),

    case eleveldb:close(DB) of
        ok           -> ok;
        {error, Err} ->
            rabbit_log:error("Unable to stop message store index"
                             " for directory ~p.~nError: ~p~n",
                             [filename:dirname(Dir), Err])
    end.


maybe_update_index(Key, Obj, State) ->
    RecoverIndex = State#state.recovery_index,
    case RecoverIndex of
        undefined -> ok;
        _         ->
            case Obj#msg_location.file of
                undefined ->
                    ok = eleveldb:put(RecoverIndex, Key, <<>>, []);
                _ ->
                    ok = eleveldb:delete(RecoverIndex, Key, [])
            end
    end.

maybe_delete_index(Key, State) ->
    RecoverIndex = State#state.recovery_index,
    case RecoverIndex of
        undefined ->
            ok;
        _ ->
            ok = eleveldb:delete(State#state.recovery_index, Key, [])
    end.

clear_index(State) ->
    % NOOP.
    % delete_by_file should be called only once during recovery!
    not_found = eleveldb:get(State#state.recovery_index, <<"RESERVED">>, []),
    ok = eleveldb:put(State#state.recovery_index, <<"RESERVED">>, <<>>, []),
    rabbit_log:error("Remove me!!").

decode_val(Val) ->
    binary_to_term(Val).

encode_val(Obj) ->
    term_to_binary(Obj).

update_elements(Old, Updates) ->
    lists:foldl(fun(Rec, {Index, Val}) ->
                    erlang:setelement(Index, Rec, Val)
                end,
                Old,
                Updates).