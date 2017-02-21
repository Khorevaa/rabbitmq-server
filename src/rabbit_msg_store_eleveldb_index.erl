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
         delete_object/2, cleanup_undefined_file/1, terminate/1]).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(INDEX_DIR, "eleveldb").
-define(TMP_RECOVER_INDEX, "recover_no_file").

-record(internal_state, {
    db,
    dir,
    read_options,
    write_options,
    recovery_index
}).

-record(index_state, {
    db,
    server,
    read_options
}).

-define(OPEN_OPTIONS, [{create_if_missing, true}, {write_buffer_size, 5242880}]).
-define(READ_OPTIONS, []).
-define(WRITE_OPTIONS, []).

%% rabbit_msg_store_index API

-type index_state() :: #index_state{}.

-spec new(file:filename()) -> index_state().
new(BaseDir) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [BaseDir, ?READ_OPTIONS, new], []),
    index_state(Pid, ?READ_OPTIONS).

-spec recover(file:filename()) -> {ok, index_state()} || {error, term()}.
recover(BaseDir) ->
    case gen_server:start_link(?MODULE, [BaseDir, ?READ_OPTIONS, recover], []) of
        {ok, Pid}    -> {ok, index_state(Pid, ?READ_OPTIONS)};
        {error, Err} -> {error, Err}
    end.

-spec lookup(rabbit_types:msg_id(), index_state()) -> ('not_found' | tuple()).
lookup(MsgId, #internal_state{db = DB, read_options = ReadOptions}) ->
    do_lookup(MsgId, DB, ReadOptions);
lookup(MsgId, #index_state{db = DB, read_options = ReadOptions}) ->
    do_lookup(MsgId, DB, ReadOptions).

%% Inserts are executed by a message store process only.
%% GC cannot call insert,
%% Clients cannot call insert

% fail if object exists
-spec insert(tuple(), index_state()) -> 'ok'.
insert(Obj, #index_state{ server = Server }) ->
    MsgId = get_msg_id(Obj),
    Val = encode_val(Obj),
%% We need the file to update recovery index.
%% File can be undefined, in that case it
%% should be deleted at the end of a recovery
%% File can become defined, and should not be deleted.
    File = file(Obj),
    gen_server:call(Server, {insert, MsgId, Val, File}).

%% Updates are executed by a message store process, just like inserts.

-spec update(tuple(), index_state()) -> 'ok'.
update(Obj, #index_state{ server = Server }) ->
    MsgId = get_msg_id(Obj),
    Val = encode_val(Obj),
    File = file(Obj),
    gen_server:call(Server, {update, MsgId, Val, File}).

%% update_fields can be executed by message store or GC

% fail if object does not exist
-spec update_fields(rabbit_types:msg_id(), ({fieldpos(), fieldvalue()} |
                                            [{fieldpos(), fieldvalue()}]),
                        index_state()) -> 'ok'.
update_fields(MsgId, Updates, #index_state{ server = Server }) ->
    gen_server:call(Server, {update_fields, MsgId, Updates}).

%% Deletes are performed by message store only, GC is using delete_object

-spec delete(rabbit_types:msg_id(), index_state()) -> 'ok'.
delete(MsgId, #index_state{ server = Server }) ->
    gen_server:call(Server, {delete, MsgId}).

%% Delete object is performed by GC

% do not delete different object
-spec delete_object(tuple(), index_state()) -> 'ok'.
delete_object(Obj, #index_state{ server = Server }) ->
    MsgId = get_msg_id(Obj),
    Val = encode_val(Obj),
    gen_server:call(Server, {delete_object, MsgId, Val}).

%% cleanup_undefined_file is called by message store after recovery from scratch

-spec cleanup_undefined_file(index_state()) -> 'ok'.
cleanup_undefined_file(#state{ server = Server }) ->
    gen_server:call(Server, cleanup_undefined_file).

-spec terminate(index_state()) -> any().
terminate(#index_state{ server = Server }) ->
    ok = gen_server:stop(Server).

%% ------------------------------------

%% Gen-server API

%% Non-clean shutdown. We create recovery index
init([BaseDir, ReadOptions, new]) ->
    % TODO: recover after crash
    Dir = index_dir(BaseDir),
    rabbit_file:recursive_delete([Dir]),
    {ok, RecoverIndex} = init_recovery_index(BaseDir),
    {ok, DbRef} = eleveldb:open(Dir, ?OPEN_OPTIONS),
    {ok, #internal_state(DbRef, Dir, ReadOptions, RecoverIndex)};

%% Clean shutdown. We don't need recovery index
init([BaseDir, recover]) ->
    Dir = index_dir(BaseDir),
    case eleveldb:open(Dir, ?OPEN_OPTIONS) of
        {ok, DbRef}  -> {ok, internal_state(DbRef, Dir, ReadOptions, undefined)};
        {error, Err} -> {stop, Err}
    end.

handle_call({insert, MsgId, Val, File}, _From,
            #state{db = DB, read_options = ReadOptions} = State) ->
    %% TODO in case of insert, we will fail to update an entry
    not_found = lookup(MsgId, State),
    {reply, do_insert(MsgId, Val, File, State), State};

handle_call({update, MsgId, Val, File}, _From, State) ->
    {reply, do_insert(MsgId, Val, File, State), State};

handle_call({update_fields, MsgId, Updates}, _From, State) ->
    #msg_location{} = Old = lookup(MsgId, State),
    New = update_elements(Old, Updates),
    File = New#msg_location.file,
    {reply, do_insert(MsgId, encode_val(New), File, State), State};

handle_call({delete, MsgId}, _From, State) ->
    do_delete(MsgId, State),
    {reply, ok, State};

handle_call({delete_object, MsgId, Val}, _From,
            #internal_state{db = DB,
                            read_options = ReadOptions,
                            write_options = WriteOptions} = State) ->
    case eleveldb:get(DB, MsgId, ReadOptions) of
        {ok, Val} ->
            do_delete(MsgId, State);
        _ -> ok
    end,
    {reply, ok, State};

handle_call(cleanup_undefined_file, _From,
            #internal_state{db = DB,
                            recovery_index = RecoveryIndex,
                            read_options = ReadOptions,
                            write_options = WriteOptions} = State) ->
    eleveldb:fold_keys(
        RecoveryIndex,
        fun(Key, nothing) ->
            ok = eleveldb:delete(DB, MsgId, WriteOptions)
        end,
        nothing,
        ReadOptions),
    {reply, ok, clear_index(State)}.

%% ------------------------------------

do_insert(MsgId, Val, File, State) ->
    DB = State#internal_state.db,
    rabbit_log:error("Write index ~p~n", [{MsgId, DB}]),
    maybe_update_recovery_index(MsgId, File, State),
    ok = eleveldb:put(DB, MsgId, Val, DB).

do_lookup(MsgId, DB, ReadOptions) ->
    rabbit_log:error("Read index ~p~n", [{MsgId, DB}]),
    case eleveldb:get(DB, MsgId, ReadOptions) of
        not_found    -> not_found;
        {ok, Val}    -> decode_val(Val);
        {error, Err} -> {error, Err}
    end.

do_delete(MsgId, #internal_state{ db = DB, write_options = WriteOptions} = State) ->
    rabbit_log:error("Delete index ~p~n", [{MsgId, DB}]),
    maybe_delete_recovery_index(MsgId, State),
    ok = eleveldb:delete(DB, MsgId, WriteOptions).

clear_index(#internal_state{recovery_index = RecoveryIndex, dir = Dir} = State) ->
    ok = eleveldb:close(RecoveryIndex),
    ok = eleveldb:destroy(recover_index_dir(filename:dirname(Dir)),
                          ?OPEN_OPTIONS),
    State#internal_state{recovery_index = undefined}.

index_dir(BaseDir) ->
    filename:join(BaseDir, ?INDEX_DIR).

recover_index_dir(BaseDir) ->
    filename:join(BaseDir, ?TMP_RECOVER_INDEX).

init_recovery_index(BaseDir) ->
    RecoverNoFileDir = recover_index_dir(BaseDir),
    rabbit_file:recursive_delete([RecoverNoFileDir]),
    eleveldb:open(RecoverNoFileDir, ?OPEN_OPTIONS).

index_state(Pid, ReadOptions) ->
    {ok, DB} = gen_server:call(Pid, reference),
    #index_state{db = DB, server = Pid, read_options = ReadOptions}.

internal_state(DbRef, Dir, ReadOptions, RecoverIndex) ->
    #internal_state{
        db = DbRef,
        dir = Dir,
        read_options = ReadOptions,
        write_options = ?WRITE_OPTIONS,
        recovery_index = RecoverIndex }.



update_fields(Key, Updates, State) ->
% TODO: pass initial object to avoid read
    Old = lookup(Key, State),
    New = update_elements(Old, Updates),
    update(New, State).


cleanup_undefined_file(undefined, State) ->
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
    RecoverNoFileDir = filename:join(filename:dirname(Dir), ?TMP_RECOVER_INDEX),
    eleveldb:destroy(RecoverNoFileDir, [{write_buffer_size, 5242880}]),

    case eleveldb:close(DB) of
        ok           -> ok;
        {error, Err} ->
            rabbit_log:error("Unable to stop message store index"
                             " for directory ~p.~nError: ~p~n",
                             [filename:dirname(Dir), Err])
    end.


maybe_update_recovery_index(Key, Obj, State) ->
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

maybe_delete_recovery_index(Key, State) ->
    RecoverIndex = State#state.recovery_index,
    case RecoverIndex of
        undefined ->
            ok;
        _ ->
            ok = eleveldb:delete(State#state.recovery_index, Key, [])
    end.

clear_index(State) ->
    % NOOP.
    % cleanup_undefined_file should be called only once during recovery!
    not_found = eleveldb:get(State#state.recovery_index, <<"RESERVED">>, []),
    ok = eleveldb:put(State#state.recovery_index, <<"RESERVED">>, <<>>, []),
    rabbit_log:error("Remove me!!").

decode_val(Val) ->
    binary_to_term(Val).

encode_val(Obj) ->
    term_to_binary(Obj).

update_elements(Old, Update) when is_tuple(Update) ->
    update_elements(Old, [Update]);
update_elements(Old, Updates) when is_list(Updates) ->
    lists:foldl(fun(Rec, {Index, Val}) ->
                    erlang:setelement(Index, Rec, Val)
                end,
                Old,
                Updates).