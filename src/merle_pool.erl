-module(merle_pool).

-export([
    create/1, delete/1, join/3,
    set_cached_checkout_state/3,
    cached_checkout_state/2,
    clean_locks/0,
    get_client/3
]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(PIDS_TABLE, merle_pool_pids).
-define(INDICES_TABLE, merle_pool_indices).
-define(CLIENTS_STATE_TABLE, merle_pool_client_states).

-define(CLEAN_LOCKS_INTERVAL, 10000). % every 10 seconds

-record(server_state, {
    pools,
    periodic_lock_clean
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

delete(Name) ->
    gen_server:call(?MODULE, {delete, Name}).

create(Name) ->
    gen_server:call(?MODULE, {create, Name}).

join(Name, Index, Pid) when is_pid(Pid) ->
    gen_server:call(?MODULE, {join, Name, Index, Pid}).

clean_locks() ->
    NowSecs = now_secs(),
    CleanLocksIntervalSecs = ?CLEAN_LOCKS_INTERVAL div 1000,

    {Cleaned, Connections} = ets:foldl(
        fun({{_Name, _Index}, Client}, {NumCleaned, ValidConnections}) ->
            % clear locks with stale last_unlocked time
            NumCleaned2 = case merle_client:get_checkout_state(Client) of
                {false, _} ->
                    NumCleaned;

                {true, indefinite} ->
                    NumCleaned;

                {true, CheckOutTime} ->
                    case (CheckOutTime + CleanLocksIntervalSecs) < NowSecs of
                        true ->
                            merle_client:checkin(Client),
                            NumCleaned + 1;
                        false ->
                            NumCleaned
                    end
            end,

            % maintain a count of the number of valid connections
            ValidConnections2 = case merle_client:get_socket(Client) of
                undefined -> ValidConnections;
                _ -> ValidConnections + 1
            end,

            {NumCleaned2, ValidConnections2}
        end,

        {0, 0}, 

        ?PIDS_TABLE
    ),
    
    lager:error("Cleaned ~p merle locks on ~p valid connections", [Cleaned, Connections]),
    
    {Cleaned, Connections}.


get_client(round_robin, Name, NumConnections) ->
    % Get the round robin index
    RRIndex = shift_rr_index(Name, NumConnections),

    case ets:lookup(?PIDS_TABLE, {Name, RRIndex}) of
        [] ->
            {error, no_client};
        [{_Key, Client}] ->
            case is_process_alive(Client) of
                true -> {client, Client, cached_checkout_state(Name, RRIndex)};
                false -> {error, client_dead}
            end
    end.


set_cached_checkout_state(Name, Index, CheckedOut) ->
    % insert new pid into the table
    ets:insert(?CLIENTS_STATE_TABLE, {{Name, Index}, CheckedOut}).


cached_checkout_state(Name, Index) ->
    case ets:lookup(?CLIENTS_STATE_TABLE, {Name, Index}) of
        [] -> false;
        [{_Key, CheckedOut}] -> CheckedOut
    end.


%%
%%  SERVER FUNCTIONS
%%

init([]) ->
    lager:info("Merle pool STARTING!"),

    process_flag(trap_exit, true),

    ets:new(?PIDS_TABLE, [set, public, named_table, {read_concurrency, true}]),
    ets:new(?INDICES_TABLE, [set, public, named_table, {read_concurrency, true}, {write_concurrency, true}]),
    ets:new(?CLIENTS_STATE_TABLE, [set, public, named_table, {read_concurrency, true}, {write_concurrency, true}]),

    PLC = timer:apply_interval(?CLEAN_LOCKS_INTERVAL, merle_pool, clean_locks, []),

    State = #server_state {
        pools = [],
        periodic_lock_clean = PLC
    },

    {ok, State}.

handle_call({create, Name}, _From, S = #server_state{pools = Pools}) ->
    S2 = case has_pool(Name, Pools) of
        false ->
            ets:insert(?INDICES_TABLE, {{Name, rr_index}, 1}),
            S#server_state{pools = [{Name, 0} | Pools]};
        true ->
            S
    end,

    {reply, ok, S2};

handle_call({join, Name, Index, Pid}, _From, S = #server_state{pools = Pools}) ->
    case has_pool(Name, Pools) of
        false ->
            {reply, no_such_group, S};
        true ->
            lager:info("Client is joining pool ~p at index ~p", [Name, Index]),

            % insert new pid into the table
            ets:insert(?PIDS_TABLE, {{Name, Index}, Pid}),

            % NOTE: link processes on join, so that if client dies, we know about it,
            % ALSO: if I die, the clients will all restart as well
            link(Pid),

            {reply, ok, S#server_state{pools = inc_pool(Name, Pools)}}
    end.
            
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({'EXIT', Pid, Reason} , S) ->
    lager:error("Caught merle_client EXIT, this shouldn't happen, ~p", [[Pid, Reason]]),
    {stop, Reason, S};

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #server_state{ periodic_lock_clean=PLC }) ->
    lager:error("Merle pool TERMINATING!"),

    ets:delete(?PIDS_TABLE),
    ets:delete(?INDICES_TABLE),
    ets:delete(?CLIENTS_STATE_TABLE),

    timer:cancel(PLC),
    
    %%do not unlink, if this fails, dangling processes should be killed
    ok.


%%
%%  HELPER FUNCTIONS
%%

shift_rr_index(Name, NumConnections) ->
    try
        ets:update_counter(?INDICES_TABLE, {Name, rr_index}, {2, 1, NumConnections, 1})
    catch
        _:_ ->
            lager:error("Error shifting merle index for name ~p", [Name]),
            0
    end.

has_pool(Name, Pools) ->
    case proplists:get_value(Name, Pools) of
        undefined -> false;
        _ -> true
    end.

inc_pool(Name, Pools) ->
    PoolSize = proplists:get_value(Name, Pools),
    lists:keyreplace(Name, 1, Pools, {Name, PoolSize+1}).

now_secs() ->
    {NowMegaSecs, NowSecs, _} = erlang:now(),
    (1.0e+6 * NowMegaSecs) + NowSecs.
