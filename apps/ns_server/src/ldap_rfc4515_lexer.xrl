Definitions.

STR = [A-Za-z0-9\-\.\s\\%]+

Rules.

\:[dD][nN]    : {token, {list_to_atom(string:to_lower(TokenChars)), TokenLine}}.
\~=           : {token, {list_to_atom(TokenChars), TokenLine}}.
>=            : {token, {list_to_atom(TokenChars), TokenLine}}.
<=            : {token, {list_to_atom(TokenChars), TokenLine}}.
\:=           : {token, {list_to_atom(TokenChars), TokenLine}}.
=\*           : {token, {list_to_atom(TokenChars), TokenLine}}.
[()&\|!=\:\*] : {token, {list_to_atom(TokenChars), TokenLine}}.
{STR}         : {token, {str, TokenLine, TokenChars}}.

Erlang code.
