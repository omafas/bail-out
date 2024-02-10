
Nonterminals
filter and or not filtercomp item filterlist simple substring extensible
initial final any attr value matchingrule.

Terminals
'(' ')' '&' '|' '!' '=' '~=' '>=' '<=' ':dn' ':' ':=' '*' '=*' str.

Rootsymbol filter.


filter     -> '(' filtercomp ')' : '$2'.
filtercomp -> and : '$1'.
filtercomp -> or : '$1'.
filtercomp -> not : '$1'.
filtercomp -> item : '$1'.
and        -> '&' filterlist : eldap:'and'('$2').
or         -> '|' filterlist : eldap:'or'('$2').
not        -> '!' filter : eldap:'not'('$2').
filterlist -> filter : ['$1'].
filterlist -> filter filterlist : ['$1'|'$2'].
item       -> simple : '$1'.
item       -> substring : '$1'.
item       -> extensible : '$1'.
simple     -> attr '=*' : eldap:present('$1').
simple     -> attr '=' value : eldap:equalityMatch('$1', '$3').
simple     -> attr '~=' value : eldap:approxMatch('$1', '$3').
simple     -> attr '>=' value : eldap:greaterOrEqual('$1', '$3').
simple     -> attr '<=' value : eldap:lessOrEqual('$1', '$3').

extensible -> attr ':dn' ':' matchingrule ':=' value : eldap:extensibleMatch('$6', [{type, '$1'}, {matchingRule, '$4'}, {dnAttributes, true}]).
extensible -> attr ':dn' ':=' value : eldap:extensibleMatch('$4', [{type, '$1'}, {dnAttributes, true}]).
extensible -> attr ':' matchingrule ':=' value : eldap:extensibleMatch('$5', [{type, '$1'}, {matchingRule, '$3'}]).
extensible -> attr ':=' value : eldap:extensibleMatch('$3', [{type, '$1'}]).
extensible -> ':dn' ':' matchingrule ':=' value : eldap:extensibleMatch('$5', [{matchingRule, '$3'}, {dnAttributes, true}]).
extensible -> ':dn' ':=' value : eldap:extensibleMatch('$3', [{dnAttributes, true}]).
extensible -> ':' matchingrule ':=' value : eldap:extensibleMatch('$4', [{matchingRule, '$2'}]).

substring -> attr '=' initial '*' : eldap:substrings('$1', ['$3']).
substring -> attr '=' initial '*' any : eldap:substrings('$1', ['$3' | '$5']).
substring -> attr '=*' any : eldap:substrings('$1', '$3').
any -> value '*' : [{any, '$1'}].
any -> value '*' any : [{any, '$1'}|'$3'].
any -> final : ['$1'].

initial    -> value : {initial, '$1'}.
final      -> value : {final, '$1'}.

attr       -> str : get_value('$1').
matchingrule -> str : get_value('$1').
value      -> str : unescape(get_value('$1'), get_line('$1'), "").

Erlang code.

get_value({_,_,V}) -> V.
get_line({_, L, _}) -> L.

unescape("\\\\" ++ T, L, R) -> unescape(T, L, "\\" ++ R);
unescape("\\" ++ [C1, C2 | T], L, R) ->
    try list_to_integer([C1, C2], 16) of
        V -> unescape(T, L, [V | R])
    catch
        _:_ ->
            return_error(L, ["invalid hex digits after escape character \\",
                             [C1, C2]])
    end;
unescape("\\" ++ _, L, _R) ->
    return_error(L, "missing hex digits after escape character \\");
unescape([C | T], L, R) -> unescape(T, L, [C | R]);
unescape([], _, R) -> lists:reverse(R).

