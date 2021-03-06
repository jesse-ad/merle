-module(merle_client).

-export([start_link/1, init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2]).

-export([checkout/3, checkin/1, get_checkout_state/1, get_socket/1]).

-define(RESTART_INTERVAL, 5000). %% retry each 5 seconds.

-record(state, {
    host,
    port,

    socket,                 % memcached connection socket

    monitor,                % represents a monitor bw checking out process and me

    checked_out,            % boolean indicating whether this connection is checked out or not
    check_out_time          % timestamp marking when this connection was checked out
}).


start_link([Host, Port]) ->
    gen_server:start_link(?MODULE, [Host, Port], []).


init([Host, Port]) ->
    log4erl:info("Merle watcher initialized!"),
    erlang:process_flag(trap_exit, true),

    merle_pool:create({Host, Port}),
    merle_pool:join({Host, Port}, self()),

    {
        ok,
        check_in_state(
            #state{
                host = Host,
                port = Port
            }
        )
    }.


%%
%%  API
%%


checkout(Pid, BorrowerPid, CheckoutTime) ->
    gen_server:call(Pid, {checkout, BorrowerPid, CheckoutTime}).


checkin(Pid) ->
    gen_server:call(Pid, checkin).


get_checkout_state(Pid) ->
    gen_server:call(Pid, get_checkout_state).


get_socket(Pid) ->
    gen_server:call(Pid, get_socket).


%%
%%  SERVER CALL HANDLERS
%%


%%
%%  Handle checkout events.  Mark this server as used, and note the time.
%%  Bind a monitor with the checking out process.
%%
handle_call({checkout, _, _}, _From, State = #state{checked_out = true}) ->
    {reply, busy, State};
handle_call({checkout, _, _}, _From, State = #state{socket = undefined}) ->
    % NOTE: initializes socket when none found
    {reply, no_socket, connect_socket(State)};
handle_call({checkout, BorrowerPid, CheckoutTime}, _From, State = #state{socket = Socket, monitor = PrevMonitor}) ->
    % handle any previously existing monitors
    case PrevMonitor of
        undefined ->
            ok;
        _ ->
            true = erlang:demonitor(PrevMonitor)
    end,

    Monitor = erlang:monitor(process, BorrowerPid),

    {reply, Socket, check_out_state(State#state{monitor = Monitor}, CheckoutTime)};


%%
%%  Handle checkin events.  Demonitor perviously monitored process, and mark as checked in
%%
handle_call(checkin, _From, State = #state{monitor = PrevMonitor}) ->
    case PrevMonitor of
        undefined -> ok;
        _ ->
            true = erlang:demonitor(PrevMonitor)
    end,

    {reply, ok, check_in_state(State#state{monitor = undefined})};


%%
%%  Returns checkout state for the client in question
%%
handle_call(get_checkout_state, _From, State = #state{checked_out = CheckedOut, check_out_time = CheckOutTime}) ->
    {reply, {CheckedOut, CheckOutTime}, State};


%%
%%  Returns socket for the client in question
%%
handle_call(get_socket, _From, State = #state{socket = Socket}) ->
    {reply, Socket, State};


handle_call(_Call, _From, S) ->
    {reply, ok, S}.


%%
%%  Handles 'connect' messages -> initializes socket on host/port, saving a reference
%%
handle_info('connect', #state{host = Host, port = Port, checked_out = true, socket = undefined} = State) ->
    case merle:connect(Host, Port) of
        {ok, Socket} ->

            {noreply, check_in_state(State#state{socket = Socket})};

        {error, Reason} ->
            error_logger:error_report([memcached_connection_error,
                {reason, Reason},
                {host, Host},
                {port, Port},
                {restarting_in, ?RESTART_INTERVAL}]
            ),
	        
	        timer:send_after(?RESTART_INTERVAL, self(), 'connect'),
	        
            {noreply, State}
   end;


%%
%%  Handles down events from monitored process.  Need to check back in if this happens.
%%
handle_info({'DOWN', MonitorRef, _, _, _}, #state{monitor=MonitorRef} = S) ->
    log4erl:info("merle_watcher caught a DOWN event"),
    
    true = erlang:demonitor(MonitorRef),

    {noreply, check_in_state(S#state{monitor = undefined})};


%%
%%  Handles exit events on the memcached socket.  If this occurs need to reconnect.
%%
handle_info({'EXIT', Socket, _}, S = #state{socket = Socket}) ->
    {noreply, connect_socket(S), ?RESTART_INTERVAL};


handle_info(_Info, S) ->
    error_logger:warning_report([{merle_watcher, self()}, {unknown_info, _Info}]),
    {noreply, S}.
    

handle_cast(_Cast, S) ->
    {noreply, S}.
    

terminate(_Reason, #state{socket = undefined}) ->
    log4erl:error("Merle watcher terminated, socket is empty!"),
    ok;

terminate(_Reason, #state{socket = Socket}) ->
    log4erl:error("Merle watcher terminated, killing socket!"),
    erlang:exit(Socket, watcher_died),
    ok.

%%
%%  HELPER FUNCTIONS
%%

connect_socket(State = #state{}) ->
    self() ! 'connect',
    check_out_state_indefinitely(State#state{socket = undefined}).


check_out_state_indefinitely(State = #state{}) ->
    check_out_state(State, indefinite).


check_out_state(State = #state{}, CheckOutTime) ->
    State#state{
        checked_out = true,
        check_out_time = CheckOutTime
    }.


check_in_state(State = #state{}) ->
    State#state{
        checked_out = false,
        check_out_time = undefined
    }.