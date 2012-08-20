REBAR = ./rebar

.PHONY: all compile clean get-deps

all: compile

compile:
	@$(REBAR) get-deps compile

clean:
	@$(REBAR) clean