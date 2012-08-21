-module(merle_cluster).

-export([configure/2, exec/4]).

-define(BUFFER_TABLE_NAME, merle_buffer).

index_map(F, List) ->
    {Map, _} = lists:mapfoldl(fun(X, Iter) -> {F(X, Iter), Iter +1} end, 1, List),
    Map.

configure(MemcachedHosts, ConnectionsPerHost) ->
    ets:new(?BUFFER_TABLE_NAME, [set, public, named_table]),
    
    SortedMemcachedHosts = lists:sort(MemcachedHosts),
    DynModuleBegin = "-module(merle_cluster_dynamic).
        -export([get_server/1]).
        get_server(ClusterKey) -> N = erlang:phash2(ClusterKey, ~p), 
            do_get_server(N).\n",
    DynModuleMap = "do_get_server(~p) -> ~p; ",
    DynModuleEnd = "do_get_server(_N) -> throw({invalid_server_slot, _N}).\n",
    
    ModuleString = lists:flatten([
        io_lib:format(DynModuleBegin, [length(SortedMemcachedHosts)]),
        index_map(
            fun([Host, Port], I) -> 
                ServerName = merle_sup:server_name(Host, Port),
                ets:insert(?BUFFER_TABLE_NAME, {ServerName, ConnectionsPerHost}),
                io_lib:format(DynModuleMap, [I-1, ServerName])
            end, 
            SortedMemcachedHosts
        ),
        DynModuleEnd
    ]),
    
    log4erl:error("dyn module str ~p", [ModuleString]),
    
    {M, B} = dynamic_compile:from_string(ModuleString),
    code:load_binary(M, "", B).

    %
    % NOTE: do not need to explicitly start children anymore... poolboy will handle this
    % 
    
    %lists:foreach(
    %    fun([Host, Port]) ->
    %        lists:foreach(
    %            fun(_) -> 
    %                supervisor:start_child(merle_sup, [[Host, Port]]) 
    %            end,
    %            lists:seq(1, ConnectionsPerHost)
    %        )
    %    end, 
    %    SortedMemcachedHosts
    %).

%%
%% Executes specified function, choosing some connection from the connection pool, then running function
%% then returning back to the pool
%%
exec(Key, Fun, FullDefault, ConnectionTimeout) ->
    S = merle_cluster_dynamic:get_server(Key),

    BufferCounter = ets:update_counter(?BUFFER_TABLE_NAME, S, -1),
    
    exec(BufferCounter, S, Key, Fun, FullDefault, ConnectionTimeout).
        
            
exec(BufferCounter, ServerName, _Key, _Fun, FullDefault, _ConnectionTimeout) when BufferCounter < 0 ->
    ets:update_counter(?BUFFER_TABLE_NAME, ServerName, 1),
    FullDefault;

exec(_BufferCounter, ServerName, Key, Fun, FullDefault, ConnectionTimeout) ->
    FromPid = self(),

    ConnFetchPid = spawn(
        fun() -> 
            
            MonitorRef = erlang:monitor(process, FromPid),

            FromPid ! {merle_conn, poolboy:checkout(ServerName, false)},
            
            receive
                {'DOWN', MonitorRef, _, _, _} -> 
                    log4erl:error("Merle connection fetch process received 'DOWN' message"),
                    ets:update_counter(?BUFFER_TABLE_NAME, ServerName, 1),
                    ok;
                done -> 
                    ok;
                Other -> 
                    log4erl:error("Merle connection unexpected message ~p", [Other])
            after 1000 ->
                log4erl:error("Merle connection fetch process timed out")
            end,
            
            true = erlang:demonitor(MonitorRef)
        end
    ),

    ReturnValue = receive 
        {merle_conn, full} ->
            log4erl:error("Merle pool is empty!"),

            ConnFetchPid ! done,

            FullDefault;
        
        {merle_conn, P} ->
            Value = Fun(P, Key),

            poolboy:checkin(ServerName, P),

            ConnFetchPid ! done,

            Value;

        after ConnectionTimeout ->
    
            exit(ConnFetchPid, kill),
    
            FullDefault
    end,
    

    ets:update_counter(?BUFFER_TABLE_NAME, ServerName, 1),

    ReturnValue.
    