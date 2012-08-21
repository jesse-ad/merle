-module(merle_cluster).

-export([configure/2, exec/4]).

index_map(F, List) ->
    {Map, _} = lists:mapfoldl(fun(X, Iter) -> {F(X, Iter), Iter +1} end, 1, List),
    Map.

configure(MemcachedHosts, _ConnectionsPerHost) ->
    SortedMemcachedHosts = lists:sort(MemcachedHosts),
    DynModuleBegin = "-module(merle_cluster_dynamic).
        -export([get_server/1]).
        get_server(ClusterKey) -> N = erlang:phash2(ClusterKey, ~p), 
            do_get_server(N).\n",
    DynModuleMap = "do_get_server(~p) -> ~p; ",
    DynModuleEnd = "do_get_server(_N) -> throw({invalid_server_slot, _N}).\n",
    
    ModuleString = lists:flatten([
        io_lib:format(DynModuleBegin, [length(SortedMemcachedHosts)]),
        index_map(fun([Host, Port], I) -> io_lib:format(DynModuleMap, [I-1, merle_sup:server_name(Host, Port)]) end, SortedMemcachedHosts),
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

    FromPid = self(),

    ConnFetchPid = spawn(
        fun() -> 
            
            MonitorRef = erlang:monitor(process, FromPid),

            FromPid ! {merle_conn, poolboy:checkout(S, false)},
            
            receive
                {'DOWN', MonitorRef, _, _, _} -> 
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
            FullDefault;
        
        {merle_conn, P} ->
            Value = Fun(P, Key),
            poolboy:checkin(S, P),
            Value

        after ConnectionTimeout ->
            FullDefault
    end,
    
    ConnFetchPid ! done,

    ReturnValue.