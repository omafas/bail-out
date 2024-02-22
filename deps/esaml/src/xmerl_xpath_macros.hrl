%% esaml - SAML for erlang
%%
%% Copyright (c) 2013, Alex Wilson and the University of Queensland
%% All rights reserved.
%%
%% Distributed subject to the terms of the 2-clause BSD license, see
%% the LICENSE file in the root of the distribution.

-define(xpath_generic(XPath, Record, Field, TransFun, TargetType, NotFoundRet),
	fun(Resp) ->
        case xmerl_xpath:string(XPath, Xml, [{namespace, Ns}]) of
            [#TargetType{value = V}] -> Resp#Record{Field = TransFun(V)};
            _ -> NotFoundRet
        end
    end).

-define(xpath_generic(XPath, Record, Field, TargetType, NotFoundRet),
	fun(Resp) ->
        case xmerl_xpath:string(XPath, Xml, [{namespace, Ns}]) of
            [#TargetType{value = V}] -> Resp#Record{Field = V};
            _ -> NotFoundRet
        end
    end).

-define(xpath_generic_many(XPath, Record, Field, TransFun, TargetType, NotFoundRet),
    fun(Resp) ->
        case xmerl_xpath:string(XPath, Xml, [{namespace, Ns}]) of
            L when is_list(L) ->
                Res = [TransFun(V) || #TargetType{value = V} <- L],
                Resp#Record{Field = Res};
            _ ->
                NotFoundRet
        end
    end).

-define(xpath_attr(XPath, Record, Field),
    ?xpath_generic(XPath, Record, Field, xmlAttribute, Resp)).
-define(xpath_attr(XPath, Record, Field, TransFun),
    ?xpath_generic(XPath, Record, Field, TransFun, xmlAttribute, Resp)).

-define(xpath_attr_required(XPath, Record, Field, Error),
    ?xpath_generic(XPath, Record, Field, xmlAttribute, {error, Error})).
-define(xpath_attr_required(XPath, Record, Field, TransFun, Error),
    ?xpath_generic(XPath, Record, Field, TransFun, xmlAttribute, {error, Error})).

-define(xpath_text(XPath, Record, Field),
    ?xpath_generic(XPath, Record, Field, xmlText, Resp)).
-define(xpath_text(XPath, Record, Field, TransFun),
    ?xpath_generic(XPath, Record, Field, TransFun, xmlText, Resp)).
-define(xpath_text_many(XPath, Record, Field, TransFun),
    ?xpath_generic_many(XPath, Record, Field, TransFun, xmlText, Resp)).

-define(xpath_text_required(XPath, Record, Field, Error),
    ?xpath_generic(XPath, Record, Field, xmlText, {error, Error})).
-define(xpath_text_required(XPath, Record, Field, TransFun, Error),
    ?xpath_generic(XPath, Record, Field, TransFun, xmlText, {error, Error})).

-define(xpath_text_append(XPath, Record, Field, Sep),
    fun(Resp) ->
        case xmerl_xpath:string(XPath, Xml, [{namespace, Ns}]) of
            [#xmlText{value = V}] -> Resp#Record{Field = Resp#Record.Field ++ Sep ++ V};
            _ -> Resp
        end
    end).

-define(xpath_recurse(XPath, Record, Field, F),
    fun(Resp) ->
        case xmerl_xpath:string(XPath, Xml, [{namespace, Ns}]) of
            [E = #xmlElement{}] ->
                case F(E) of
                    {error, V} -> {error, V};
                    {ok, V} -> Resp#Record{Field = V}
                end;
            _ -> Resp
        end
    end).