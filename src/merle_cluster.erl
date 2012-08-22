-module(merle_cluster).

-export([configure/2, exec/4]).

index_map(F, List) ->
    {Map, _} = lists:mapfoldl(fun(X, Iter) -> {F(X, Iter), Iter +1} end, 1, List),
    Map.

configure(MemcachedHosts, ConnectionsPerHost) ->
    SortedMemcachedHosts = lists:sort(MemcachedHosts),
    DynModuleBegin = "-module(merle_cluster_dynamic).
        -export([get_server/1]).
        get_server(ClusterKey) -> N = erlang:phash2(ClusterKey, ~p), 
            do_get_server(N).\n",
    DynModuleMap = "do_get_server(~p) -> {\"~s\", ~p}; ",
    DynModuleEnd = "do_get_server(_N) -> throw({invalid_server_slot, _N}).\n",
    
    ModuleString = lists:flatten([
        io_lib:format(DynModuleBegin, [length(SortedMemcachedHosts)]),
        index_map(fun([Host, Port], I) -> io_lib:format(DynModuleMap, [I-1, Host, Port]) end, SortedMemcachedHosts),
        DynModuleEnd
    ]),
    
    log4erl:error("dyn module str ~p", [ModuleString]),
    
    {M, B} = dynamic_compile:from_string(ModuleString),
    code:load_binary(M, "", B),

    lists:foreach(
        fun([Host, Port]) ->
            lists:foreach(
                fun(_) -> 
                    supervisor:start_child(merle_sup, [[Host, Port]]) 
                end,
                lists:seq(1, ConnectionsPerHost)
            )
        end, 
        SortedMemcachedHosts
    ).


exec(Key, Fun, FullDefault, ConnectionTimeout) ->
    S = merle_cluster_dynamic:get_server(Key),

    FromPid = self(),

    ConnFetchPid = spawn(
        fun() -> 

            MerleConn = local_pg2:get_closest_pid(round_robin, S),
            MonitorRef = erlang:monitor(process, FromPid),

            FromPid ! {merle_watcher, MerleConn},

            receive
                {'DOWN', MonitorRef, _, _, _} -> 

                    log4erl:error("Merle connection fetch process received 'DOWN' message"),

                    ok;

                done -> 

                    ok;

                Other -> 

                    log4erl:error("Merle connection unexpected message ~p", [Other])

            after 1000 ->

                log4erl:error("Merle connection fetch process timed out")
                
            end,

            local_pg2:checkin_pid(MerleConn),

            true = erlang:demonitor(MonitorRef)
            
        end
    ),

    ReturnValue = receive 
        {merle_watcher, in_use} ->
            log4erl:info("Merle pool is full!"),

            ConnFetchPid ! done,

            FullDefault;

        {merle_watcher, P} ->
            log4erl:info("Merle found connection!"),

            MC = merle_watcher:merle_connection(P),
            
            Value = Fun(MC, Key),

            ConnFetchPid ! done,

            Value

        after ConnectionTimeout ->
            log4erl:error("Merle timed out while trying to retrieve connection!"),

            ConnFetchPid ! done,

            FullDefault
    end,

    ReturnValue.