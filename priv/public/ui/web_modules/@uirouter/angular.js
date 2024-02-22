import { _ as __decorate, a as __metadata, c as __param, b as __extends, g as __spread } from '../common/tslib.es6-c4a4947b.js';
import { g as from, e as map } from '../common/mergeMap-64c6f393.js';
import '../common/merge-183efbc7.js';
import { a as concat, B as BehaviorSubject, c as combineLatest } from '../common/concat-981db672.js';
import '../common/Notification-9e07e457.js';
import { R as ReplaySubject } from '../common/ReplaySubject-8316d9c1.js';
import { o as of } from '../common/filter-d76a729c.js';
import '../common/share-d41e3509.js';
import { s as switchMap } from '../common/switchMap-c513d696.js';
import { ComponentFactoryResolver, ReflectiveInjector, ViewChild, ViewContainerRef, Input, Component, Inject, Directive, HostListener, Optional, Output, ContentChildren, Host, Self, InjectionToken, ANALYZE_FOR_ENTRY_COMPONENTS, APP_INITIALIZER, NgModule, NgModuleFactoryLoader, Compiler, NgModuleFactory, PLATFORM_ID, Injector, ElementRef, Renderer2, EventEmitter, QueryList } from '../@angular/core.js';
import { LocationStrategy, HashLocationStrategy, PathLocationStrategy, CommonModule, isPlatformBrowser } from '../@angular/common.js';
import { p as pick, f as forEach, V as ViewService, s as services, a as parse, i as isFunction, R as ResolveContext, u as unnestR, b as filter, t as trace, N as NATIVE_INJECTOR_TOKEN, c as inArray, d as isDefined, e as extend, g as isNumber, h as identity, j as uniqR, P as PathUtils, k as anyTrueR, T as TransitionService, l as isString, U as UIRouter, m as Resolvable, n as parseUrl, B as BaseLocationServices, o as servicesPlugin, S as StateRegistry, q as StateService, r as UrlMatcherFactory, v as UrlRouter, w as UrlService, x as UIRouterGlobals, y as is, z as BrowserLocationConfig, A as tail, C as Param } from '../common/interface-c1256a29.js';
export { bu as $injector, bt as $q, B as BaseLocationServices, bs as BaseUrlRule, z as BrowserLocationConfig, aY as Category, a_ as DefType, ak as Glob, bv as HashLocationService, bd as HookBuilder, by as MemoryLocationConfig, bw as MemoryLocationService, N as NATIVE_INJECTOR_TOKEN, C as Param, bq as ParamFactory, b1 as ParamType, a$ as ParamTypes, b2 as PathNode, P as PathUtils, bx as PushStateLocationService, aI as Queue, bf as RegisteredHook, bh as RejectType, bi as Rejection, m as Resolvable, R as ResolveContext, b6 as StateBuilder, b8 as StateMatcher, b7 as StateObject, b0 as StateParams, b9 as StateQueueManager, S as StateRegistry, q as StateService, ba as TargetState, aZ as Trace, bj as Transition, bl as TransitionEventType, bk as TransitionHook, bb as TransitionHookPhase, bc as TransitionHookScope, T as TransitionService, U as UIRouter, x as UIRouterGlobals, bG as UIRouterPluginBase, bo as UrlConfig, bp as UrlMatcher, r as UrlMatcherFactory, v as UrlRouter, br as UrlRuleFactory, bn as UrlRules, w as UrlService, V as ViewService, ag as _extend, _ as _inArray, O as _pushTo, L as _removeFrom, at as all, a4 as allTrueR, Y as ancestors, ar as and, au as any, k as anyTrueR, ae as applyPairs, ad as arrayTuples, ab as assertFn, aa as assertMap, a9 as assertPredicate, aP as beforeAfterSubstr, bB as buildUrl, am as compose, af as copy, I as createProxyFunctions, al as curry, b4 as defaultResolvePolicy, bm as defaultTransOpts, W as defaults, Q as deregAll, av as eq, G as equals, e as extend, b as filter, a0 as find, a8 as flatten, a5 as flattenR, aN as fnToString, f as forEach, E as fromJson, aM as functionToString, bA as getParams, bD as hashLocationPlugin, aQ as hostRegex, h as identity, c as inArray, J as inherit, ax as invoke, y as is, aD as isArray, aE as isDate, d as isDefined, i as isFunction, aG as isInjectable, aA as isNull, aB as isNullOrUndefined, g as isNumber, aC as isObject, aH as isPromise, aF as isRegExp, l as isString, az as isUndefined, aX as joinNeighborsR, aL as kebobString, bz as keyValsToObjectR, bC as locationPluginFactory, bg as makeEvent, aj as makeStub, a2 as map, a1 as mapObj, be as matchState, aJ as maxLength, bF as memoryLocationPlugin, X as mergeR, H as noop, aq as not, Z as omit, as as or, aK as padString, ac as pairs, a as parse, n as parseUrl, ay as pattern, p as pick, an as pipe, $ as pluck, ao as prop, ap as propEq, a6 as pushR, bE as pushStateLocationPlugin, M as pushTo, K as removeFrom, b5 as resolvablesBuilder, b3 as resolvePolicies, D as root, s as services, o as servicesPlugin, ah as silenceUncaughtInPromise, ai as silentRejection, aU as splitEqual, aS as splitHash, aW as splitOnDelim, aT as splitQuery, aO as stringify, aR as stripLastPathElement, A as tail, F as toJson, t as trace, aV as trimHashVal, j as uniqR, a7 as unnest, u as unnestR, aw as val, a3 as values } from '../common/interface-c1256a29.js';
import { U as UIRouterRx } from '../common/ui-router-rx-04f7f595.js';

/** @module ng2 */ /** */
/**
 * This is a [[StateBuilder.builder]] function for Angular `views`.
 *
 * When the [[StateBuilder]] builds a [[State]] object from a raw [[StateDeclaration]], this builder
 * handles the `views` property with logic specific to @uirouter/angular.
 *
 * If no `views: {}` property exists on the [[StateDeclaration]], then it creates the `views` object and
 * applies the state-level configuration to a view named `$default`.
 */
function ng2ViewsBuilder(state) {
    var views = {}, viewsObject = state.views || { $default: pick(state, ['component', 'bindings']) };
    forEach(viewsObject, function (config, name) {
        name = name || '$default'; // Account for views: { "": { template... } }
        if (isFunction(config))
            config = { component: config };
        if (Object.keys(config).length === 0)
            return;
        config.$type = 'ng2';
        config.$context = state;
        config.$name = name;
        var normalized = ViewService.normalizeUIViewTarget(config.$context, config.$name);
        config.$uiViewName = normalized.uiViewName;
        config.$uiViewContextAnchor = normalized.uiViewContextAnchor;
        views[name] = config;
    });
    return views;
}
var id = 0;
var Ng2ViewConfig = /** @class */ (function () {
    function Ng2ViewConfig(path, viewDecl) {
        this.path = path;
        this.viewDecl = viewDecl;
        this.$id = id++;
        this.loaded = true;
    }
    Ng2ViewConfig.prototype.load = function () {
        return services.$q.when(this);
    };
    return Ng2ViewConfig;
}());

/**
 * Merge two injectors
 *
 * This class implements the Injector ng2 interface but delegates
 * to the Injectors provided in the constructor.
 */
var MergeInjector = /** @class */ (function () {
    function MergeInjector() {
        var injectors = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            injectors[_i] = arguments[_i];
        }
        if (injectors.length < 2)
            throw new Error('pass at least two injectors');
        this.injectors = injectors;
    }
    /**
     * Get the token from the first injector which contains it.
     *
     * Delegates to the first Injector.get().
     * If not found, then delegates to the second Injector (and so forth).
     * If no Injector contains the token, return the `notFoundValue`, or throw.
     *
     * @param token the DI token
     * @param notFoundValue the value to return if none of the Injectors contains the token.
     * @returns {any} the DI value
     */
    MergeInjector.prototype.get = function (token, notFoundValue) {
        for (var i = 0; i < this.injectors.length; i++) {
            var val = this.injectors[i].get(token, MergeInjector.NOT_FOUND);
            if (val !== MergeInjector.NOT_FOUND)
                return val;
        }
        if (arguments.length >= 2)
            return notFoundValue;
        // This will throw the DI Injector error
        this.injectors[0].get(token);
    };
    MergeInjector.NOT_FOUND = {};
    return MergeInjector;
}());

/** @hidden */
var id$1 = 0;
/**
 * Given a component class, gets the inputs of styles:
 *
 * - @Input('foo') _foo
 * - `inputs: ['foo']`
 *
 * @internalapi
 */
var ng2ComponentInputs = function (factory) {
    return factory.inputs.map(function (input) { return ({ prop: input.propName, token: input.templateName }); });
};
var ɵ0 = ng2ComponentInputs;
/**
 * A UI-Router viewport directive, which is filled in by a view (component) on a state.
 *
 * ### Selector
 *
 * A `ui-view` directive can be created as an element: `<ui-view></ui-view>` or as an attribute: `<div ui-view></div>`.
 *
 * ### Purpose
 *
 * This directive is used in a Component template (or as the root component) to create a viewport.  The viewport
 * is filled in by a view (as defined by a [[Ng2ViewDeclaration]] inside a [[Ng2StateDeclaration]]) when the view's
 * state has been activated.
 *
 * #### Example:
 * ```js
 * // This app has two states, 'foo' and 'bar'
 * stateRegistry.register({ name: 'foo', url: '/foo', component: FooComponent });
 * stateRegistry.register({ name: 'bar', url: '/bar', component: BarComponent });
 * ```
 * ```html
 * <!-- This ui-view will be filled in by the foo state's component or
 *      the bar state's component when the foo or bar state is activated -->
 * <ui-view></ui-view>
 * ```
 *
 * ### Named ui-views
 *
 * A `ui-view` may optionally be given a name via the attribute value: `<div ui-view='header'></div>`.  *Note:
 * an unnamed `ui-view` is internally named `$default`*.   When a `ui-view` has a name, it will be filled in
 * by a matching named view.
 *
 * #### Example:
 * ```js
 * stateRegistry.register({
 *   name: 'foo',
 *   url: '/foo',
 *   views: { header: HeaderComponent, $default: FooComponent });
 * ```
 * ```html
 * <!-- When 'foo' state is active, filled by HeaderComponent -->
 * <div ui-view="header"></div>
 *
 * <!-- When 'foo' state is active, filled by FooComponent -->
 * <ui-view></ui-view>
 * ```
 */
var UIView = /** @class */ (function () {
    function UIView(router, parent, viewContainerRef) {
        this.router = router;
        this.viewContainerRef = viewContainerRef;
        /** Data about the this UIView */
        this._uiViewData = {};
        this._parent = parent;
    }
    UIView_1 = UIView;
    Object.defineProperty(UIView.prototype, "_name", {
        set: function (val) {
            this.name = val;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(UIView.prototype, "state", {
        /**
         * @returns the UI-Router `state` that is filling this uiView, or `undefined`.
         */
        get: function () {
            return parse('_uiViewData.config.viewDecl.$context.self')(this);
        },
        enumerable: true,
        configurable: true
    });
    UIView.prototype.ngOnInit = function () {
        var _this = this;
        var router = this.router;
        var parentFqn = this._parent.fqn;
        var name = this.name || '$default';
        this._uiViewData = {
            $type: 'ng2',
            id: id$1++,
            name: name,
            fqn: parentFqn ? parentFqn + '.' + name : name,
            creationContext: this._parent.context,
            configUpdated: this._viewConfigUpdated.bind(this),
            config: undefined,
        };
        this._deregisterUiCanExitHook = router.transitionService.onBefore({}, function (trans) {
            return _this._invokeUiCanExitHook(trans);
        });
        this._deregisterUiOnParamsChangedHook = router.transitionService.onSuccess({}, function (trans) {
            return _this._invokeUiOnParamsChangedHook(trans);
        });
        this._deregisterUIView = router.viewService.registerUIView(this._uiViewData);
    };
    /**
     * For each transition, checks the component loaded in the ui-view for:
     *
     * - has a uiCanExit() component hook
     * - is being exited
     *
     * If both are true, adds the uiCanExit component function as a hook to that singular Transition.
     */
    UIView.prototype._invokeUiCanExitHook = function (trans) {
        var instance = this._componentRef && this._componentRef.instance;
        var uiCanExitFn = instance && instance.uiCanExit;
        if (isFunction(uiCanExitFn)) {
            var state = this.state;
            if (trans.exiting().indexOf(state) !== -1) {
                trans.onStart({}, function () {
                    return uiCanExitFn.call(instance, trans);
                });
            }
        }
    };
    /**
     * For each transition, checks if any param values changed and notify component
     */
    UIView.prototype._invokeUiOnParamsChangedHook = function ($transition$) {
        var instance = this._componentRef && this._componentRef.instance;
        var uiOnParamsChanged = instance && instance.uiOnParamsChanged;
        if (isFunction(uiOnParamsChanged)) {
            var viewState = this.state;
            var resolveContext = new ResolveContext(this._uiViewData.config.path);
            var viewCreationTrans = resolveContext.getResolvable('$transition$').data;
            // Exit early if the $transition$ is the same as the view was created within.
            // Exit early if the $transition$ will exit the state the view is for.
            if ($transition$ === viewCreationTrans || $transition$.exiting().indexOf(viewState) !== -1)
                return;
            var toParams_1 = $transition$.params('to');
            var fromParams_1 = $transition$.params('from');
            var getNodeSchema = function (node) { return node.paramSchema; };
            var toSchema = $transition$
                .treeChanges('to')
                .map(getNodeSchema)
                .reduce(unnestR, []);
            var fromSchema_1 = $transition$
                .treeChanges('from')
                .map(getNodeSchema)
                .reduce(unnestR, []);
            // Find the to params that have different values than the from params
            var changedToParams = toSchema.filter(function (param) {
                var idx = fromSchema_1.indexOf(param);
                return idx === -1 || !fromSchema_1[idx].type.equals(toParams_1[param.id], fromParams_1[param.id]);
            });
            // Only trigger callback if a to param has changed or is new
            if (changedToParams.length) {
                var changedKeys_1 = changedToParams.map(function (x) { return x.id; });
                // Filter the params to only changed/new to params.  `$transition$.params()` may be used to get all params.
                var newValues = filter(toParams_1, function (val, key) { return changedKeys_1.indexOf(key) !== -1; });
                instance.uiOnParamsChanged(newValues, $transition$);
            }
        }
    };
    UIView.prototype._disposeLast = function () {
        if (this._componentRef)
            this._componentRef.destroy();
        this._componentRef = null;
    };
    UIView.prototype.ngOnDestroy = function () {
        if (this._deregisterUIView)
            this._deregisterUIView();
        if (this._deregisterUiCanExitHook)
            this._deregisterUiCanExitHook();
        if (this._deregisterUiOnParamsChangedHook)
            this._deregisterUiOnParamsChangedHook();
        this._deregisterUIView = this._deregisterUiCanExitHook = this._deregisterUiOnParamsChangedHook = null;
        this._disposeLast();
    };
    /**
     * The view service is informing us of an updated ViewConfig
     * (usually because a transition activated some state and its views)
     */
    UIView.prototype._viewConfigUpdated = function (config) {
        // The config may be undefined if there is nothing currently targeting this UIView.
        // Dispose the current component, if there is one
        if (!config)
            return this._disposeLast();
        // Only care about Ng2 configs
        if (!(config instanceof Ng2ViewConfig))
            return;
        // The "new" viewconfig is already applied, so exit early
        if (this._uiViewData.config === config)
            return;
        // This is a new ViewConfig.  Dispose the previous component
        this._disposeLast();
        trace.traceUIViewConfigUpdated(this._uiViewData, config && config.viewDecl.$context);
        this._applyUpdatedConfig(config);
        // Initiate change detection for the newly created component
        this._componentRef.changeDetectorRef.markForCheck();
    };
    UIView.prototype._applyUpdatedConfig = function (config) {
        this._uiViewData.config = config;
        // Create the Injector for the routed component
        var context = new ResolveContext(config.path);
        var componentInjector = this._getComponentInjector(context);
        // Get the component class from the view declaration. TODO: allow promises?
        var componentClass = config.viewDecl.component;
        // Create the component
        var compFactoryResolver = componentInjector.get(ComponentFactoryResolver);
        var compFactory = compFactoryResolver.resolveComponentFactory(componentClass);
        this._componentRef = this._componentTarget.createComponent(compFactory, undefined, componentInjector);
        // Wire resolves to @Input()s
        this._applyInputBindings(compFactory, this._componentRef.instance, context, componentClass);
    };
    /**
     * Creates a new Injector for a routed component.
     *
     * Adds resolve values to the Injector
     * Adds providers from the NgModule for the state
     * Adds providers from the parent Component in the component tree
     * Adds a PARENT_INJECT view context object
     *
     * @returns an Injector
     */
    UIView.prototype._getComponentInjector = function (context) {
        // Map resolves to "useValue: providers"
        var resolvables = context
            .getTokens()
            .map(function (token) { return context.getResolvable(token); })
            .filter(function (r) { return r.resolved; });
        var newProviders = resolvables.map(function (r) { return ({ provide: r.token, useValue: context.injector().get(r.token) }); });
        var parentInject = { context: this._uiViewData.config.viewDecl.$context, fqn: this._uiViewData.fqn };
        newProviders.push({ provide: UIView_1.PARENT_INJECT, useValue: parentInject });
        var parentComponentInjector = this.viewContainerRef.injector;
        var moduleInjector = context.getResolvable(NATIVE_INJECTOR_TOKEN).data;
        var mergedParentInjector = new MergeInjector(moduleInjector, parentComponentInjector);
        return ReflectiveInjector.resolveAndCreate(newProviders, mergedParentInjector);
    };
    /**
     * Supplies component inputs with resolve data
     *
     * Finds component inputs which match resolves (by name) and sets the input value
     * to the resolve data.
     */
    UIView.prototype._applyInputBindings = function (factory, component, context, componentClass) {
        var bindings = this._uiViewData.config.viewDecl['bindings'] || {};
        var explicitBoundProps = Object.keys(bindings);
        // Returns the actual component property for a renamed an input renamed using `@Input('foo') _foo`.
        // return the `_foo` property
        var renamedInputProp = function (prop) {
            var input = factory.inputs.find(function (i) { return i.templateName === prop; });
            return (input && input.propName) || prop;
        };
        // Supply resolve data to component as specified in the state's `bindings: {}`
        var explicitInputTuples = explicitBoundProps.reduce(function (acc, key) { return acc.concat([{ prop: renamedInputProp(key), token: bindings[key] }]); }, []);
        // Supply resolve data to matching @Input('prop') or inputs: ['prop']
        var implicitInputTuples = ng2ComponentInputs(factory).filter(function (tuple) { return !inArray(explicitBoundProps, tuple.prop); });
        var addResolvable = function (tuple) { return ({
            prop: tuple.prop,
            resolvable: context.getResolvable(tuple.token),
        }); };
        var injector = context.injector();
        explicitInputTuples
            .concat(implicitInputTuples)
            .map(addResolvable)
            .filter(function (tuple) { return tuple.resolvable && tuple.resolvable.resolved; })
            .forEach(function (tuple) {
            component[tuple.prop] = injector.get(tuple.resolvable.token);
        });
    };
    var UIView_1;
    UIView.PARENT_INJECT = 'UIView.PARENT_INJECT';
    __decorate([
        ViewChild('componentTarget', { read: ViewContainerRef, static: true }),
        __metadata("design:type", ViewContainerRef)
    ], UIView.prototype, "_componentTarget", void 0);
    __decorate([
        Input('name'),
        __metadata("design:type", String)
    ], UIView.prototype, "name", void 0);
    __decorate([
        Input('ui-view'),
        __metadata("design:type", String),
        __metadata("design:paramtypes", [String])
    ], UIView.prototype, "_name", null);
    UIView = UIView_1 = __decorate([
        Component({
            selector: 'ui-view, [ui-view]',
            exportAs: 'uiView',
            template: "\n    <ng-template #componentTarget></ng-template>\n    <ng-content *ngIf=\"!_componentRef\"></ng-content>\n  "
        }),
        __param(1, Inject(UIView_1.PARENT_INJECT)),
        __metadata("design:paramtypes", [UIRouter, Object, ViewContainerRef])
    ], UIView);
    return UIView;
}());

/** @module ng2 */ /** */
function applyModuleConfig(uiRouter, injector, module) {
    if (module === void 0) { module = {}; }
    if (isFunction(module.config)) {
        module.config(uiRouter, injector, module);
    }
    var states = module.states || [];
    return states.map(function (state) { return uiRouter.stateRegistry.register(state); });
}
function applyRootModuleConfig(uiRouter, injector, module) {
    isDefined(module.deferIntercept) && uiRouter.urlService.deferIntercept(module.deferIntercept);
    isDefined(module.otherwise) && uiRouter.urlService.rules.otherwise(module.otherwise);
    isDefined(module.initial) && uiRouter.urlService.rules.initial(module.initial);
}

/**
 * @internalapi
 * # blah blah blah
 */
var AnchorUISref = /** @class */ (function () {
    function AnchorUISref(_el, _renderer) {
        this._el = _el;
        this._renderer = _renderer;
    }
    AnchorUISref.prototype.openInNewTab = function () {
        return this._el.nativeElement.target === '_blank';
    };
    AnchorUISref.prototype.update = function (href) {
        if (href && href !== '') {
            this._renderer.setProperty(this._el.nativeElement, 'href', href);
        }
        else {
            this._renderer.removeAttribute(this._el.nativeElement, 'href');
        }
    };
    AnchorUISref = __decorate([
        Directive({ selector: 'a[uiSref]' }),
        __metadata("design:paramtypes", [ElementRef, Renderer2])
    ], AnchorUISref);
    return AnchorUISref;
}());
/**
 * A directive when clicked, initiates a [[Transition]] to a [[TargetState]].
 *
 * ### Purpose
 *
 * This directive is applied to anchor tags (`<a>`) or any other clickable element.  It is a state reference (or sref --
 * similar to an href).  When clicked, the directive will transition to that state by calling [[StateService.go]],
 * and optionally supply state parameter values and transition options.
 *
 * When this directive is on an anchor tag, it will also add an `href` attribute to the anchor.
 *
 * ### Selector
 *
 * - `[uiSref]`: The directive is created as an attribute on an element, e.g., `<a uiSref></a>`
 *
 * ### Inputs
 *
 * - `uiSref`: the target state's name, e.g., `uiSref="foostate"`.  If a component template uses a relative `uiSref`,
 * e.g., `uiSref=".child"`, the reference is relative to that component's state.
 *
 * - `uiParams`: any target state parameter values, as an object, e.g., `[uiParams]="{ fooId: bar.fooId }"`
 *
 * - `uiOptions`: [[TransitionOptions]], e.g., `[uiOptions]="{ inherit: false }"`
 *
 * @example
 * ```html
 *
 * <!-- Targets bar state' -->
 * <a uiSref="bar">Bar</a>
 *
 * <!-- Assume this component's state is "foo".
 *      Relatively targets "foo.child" -->
 * <a uiSref=".child">Foo Child</a>
 *
 * <!-- Targets "bar" state and supplies parameter value -->
 * <a uiSref="bar" [uiParams]="{ barId: foo.barId }">Bar {{foo.barId}}</a>
 *
 * <!-- Targets "bar" state and parameter, doesn't inherit existing parameters-->
 * <a uiSref="bar" [uiParams]="{ barId: foo.barId }" [uiOptions]="{ inherit: false }">Bar {{foo.barId}}</a>
 * ```
 */
var UISref = /** @class */ (function () {
    function UISref(_router, _anchorUISref, parent) {
        var _this = this;
        /**
         * An observable (ReplaySubject) of the state this UISref is targeting.
         * When the UISref is clicked, it will transition to this [[TargetState]].
         */
        this.targetState$ = new ReplaySubject(1);
        /** @internalapi */ this._emit = false;
        this._router = _router;
        this._anchorUISref = _anchorUISref;
        this._parent = parent;
        this._statesSub = _router.globals.states$.subscribe(function () { return _this.update(); });
    }
    Object.defineProperty(UISref.prototype, "uiSref", {
        /** @internalapi */
        set: function (val) {
            this.state = val;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(UISref.prototype, "uiParams", {
        /** @internalapi */
        set: function (val) {
            this.params = val;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(UISref.prototype, "uiOptions", {
        /** @internalapi */
        set: function (val) {
            this.options = val;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    UISref.prototype.ngOnInit = function () {
        this._emit = true;
        this.update();
    };
    UISref.prototype.ngOnChanges = function (changes) {
        this.update();
    };
    UISref.prototype.ngOnDestroy = function () {
        this._emit = false;
        this._statesSub.unsubscribe();
        this.targetState$.unsubscribe();
    };
    UISref.prototype.update = function () {
        var $state = this._router.stateService;
        if (this._emit) {
            var newTarget = $state.target(this.state, this.params, this.getOptions());
            this.targetState$.next(newTarget);
        }
        if (this._anchorUISref) {
            var href = $state.href(this.state, this.params, this.getOptions());
            this._anchorUISref.update(href);
        }
    };
    UISref.prototype.getOptions = function () {
        var defaultOpts = {
            relative: this._parent && this._parent.context && this._parent.context.name,
            inherit: true,
            source: 'sref',
        };
        return extend(defaultOpts, this.options || {});
    };
    /** When triggered by a (click) event, this function transitions to the UISref's target state */
    UISref.prototype.go = function (button, ctrlKey, metaKey) {
        if ((this._anchorUISref &&
            (this._anchorUISref.openInNewTab() || button || !isNumber(button) || ctrlKey || metaKey)) ||
            !this.state) {
            return;
        }
        this._router.stateService.go(this.state, this.params, this.getOptions());
        return false;
    };
    __decorate([
        Input('uiSref'),
        __metadata("design:type", String)
    ], UISref.prototype, "state", void 0);
    __decorate([
        Input('uiParams'),
        __metadata("design:type", Object)
    ], UISref.prototype, "params", void 0);
    __decorate([
        Input('uiOptions'),
        __metadata("design:type", Object)
    ], UISref.prototype, "options", void 0);
    __decorate([
        HostListener('click', ['$event.button', '$event.ctrlKey', '$event.metaKey']),
        __metadata("design:type", Function),
        __metadata("design:paramtypes", [Number, Boolean, Boolean]),
        __metadata("design:returntype", void 0)
    ], UISref.prototype, "go", null);
    UISref = __decorate([
        Directive({
            selector: '[uiSref]',
            exportAs: 'uiSref',
        }),
        __param(1, Optional()),
        __param(2, Inject(UIView.PARENT_INJECT)),
        __metadata("design:paramtypes", [UIRouter,
            AnchorUISref, Object])
    ], UISref);
    return UISref;
}());

/** @internalapi */
var inactiveStatus = {
    active: false,
    exact: false,
    entering: false,
    exiting: false,
    targetStates: [],
};
/**
 * Returns a Predicate<PathNode[]>
 *
 * The predicate returns true when the target state (and param values)
 * match the (tail of) the path, and the path's param values
 *
 * @internalapi
 */
var pathMatches = function (target) {
    if (!target.exists())
        return function () { return false; };
    var state = target.$state();
    var targetParamVals = target.params();
    var targetPath = PathUtils.buildPath(target);
    var paramSchema = targetPath
        .map(function (node) { return node.paramSchema; })
        .reduce(unnestR, [])
        .filter(function (param) { return targetParamVals.hasOwnProperty(param.id); });
    return function (path) {
        var tailNode = tail(path);
        if (!tailNode || tailNode.state !== state)
            return false;
        var paramValues = PathUtils.paramValues(path);
        return Param.equals(paramSchema, paramValues, targetParamVals);
    };
};
/**
 * Given basePath: [a, b], appendPath: [c, d]),
 * Expands the path to [c], [c, d]
 * Then appends each to [a,b,] and returns: [a, b, c], [a, b, c, d]
 *
 * @internalapi
 */
function spreadToSubPaths(basePath, appendPath) {
    return appendPath.map(function (node) { return basePath.concat(PathUtils.subPath(appendPath, function (n) { return n.state === node.state; })); });
}
/**
 * Given a TransEvt (Transition event: started, success, error)
 * and a UISref Target State, return a SrefStatus object
 * which represents the current status of that Sref:
 * active, activeEq (exact match), entering, exiting
 *
 * @internalapi
 */
function getSrefStatus(event, srefTarget) {
    var pathMatchesTarget = pathMatches(srefTarget);
    var tc = event.trans.treeChanges();
    var isStartEvent = event.evt === 'start';
    var isSuccessEvent = event.evt === 'success';
    var activePath = isSuccessEvent ? tc.to : tc.from;
    var isActive = function () {
        return spreadToSubPaths([], activePath)
            .map(pathMatchesTarget)
            .reduce(anyTrueR, false);
    };
    var isExact = function () { return pathMatchesTarget(activePath); };
    var isEntering = function () {
        return spreadToSubPaths(tc.retained, tc.entering)
            .map(pathMatchesTarget)
            .reduce(anyTrueR, false);
    };
    var isExiting = function () {
        return spreadToSubPaths(tc.retained, tc.exiting)
            .map(pathMatchesTarget)
            .reduce(anyTrueR, false);
    };
    return {
        active: isActive(),
        exact: isExact(),
        entering: isStartEvent ? isEntering() : false,
        exiting: isStartEvent ? isExiting() : false,
        targetStates: [srefTarget],
    };
}
/** @internalapi */
function mergeSrefStatus(left, right) {
    return {
        active: left.active || right.active,
        exact: left.exact || right.exact,
        entering: left.entering || right.entering,
        exiting: left.exiting || right.exiting,
        targetStates: left.targetStates.concat(right.targetStates),
    };
}
/**
 * A directive which emits events when a paired [[UISref]] status changes.
 *
 * This directive is primarily used by the [[UISrefActive]] directives to monitor `UISref`(s).
 *
 * This directive shares two attribute selectors with `UISrefActive`:
 *
 * - `[uiSrefActive]`
 * - `[uiSrefActiveEq]`.
 *
 * Thus, whenever a `UISrefActive` directive is created, a `UISrefStatus` directive is also created.
 *
 * Most apps should simply use `UISrefActive`, but some advanced components may want to process the
 * [[SrefStatus]] events directly.
 *
 * ```js
 * <li (uiSrefStatus)="onSrefStatusChanged($event)">
 *   <a uiSref="book" [uiParams]="{ bookId: book.id }">Book {{ book.name }}</a>
 * </li>
 * ```
 *
 * The `uiSrefStatus` event is emitted whenever an enclosed `uiSref`'s status changes.
 * The event emitted is of type [[SrefStatus]], and has boolean values for `active`, `exact`, `entering`, and `exiting`; also has a [[StateOrName]] `identifier`value.
 *
 * The values from this event can be captured and stored on a component (then applied, e.g., using ngClass).
 *
 * ---
 *
 * A single `uiSrefStatus` can enclose multiple `uiSref`.
 * Each status boolean (`active`, `exact`, `entering`, `exiting`) will be true if *any of the enclosed `uiSref` status is true*.
 * In other words, all enclosed `uiSref` statuses  are merged to a single status using `||` (logical or).
 *
 * ```js
 * <li (uiSrefStatus)="onSrefStatus($event)" uiSref="admin">
 *   Home
 *   <ul>
 *     <li> <a uiSref="admin.users">Users</a> </li>
 *     <li> <a uiSref="admin.groups">Groups</a> </li>
 *   </ul>
 * </li>
 * ```
 *
 * In the above example, `$event.active === true` when either `admin.users` or `admin.groups` is active.
 *
 * ---
 *
 * This API is subject to change.
 */
var UISrefStatus = /** @class */ (function () {
    function UISrefStatus(_hostUiSref, _globals) {
        /** current statuses of the state/params the uiSref directive is linking to */
        this.uiSrefStatus = new EventEmitter(false);
        this._globals = _globals;
        this._hostUiSref = _hostUiSref;
        this.status = Object.assign({}, inactiveStatus);
    }
    UISrefStatus.prototype.ngAfterContentInit = function () {
        var _this = this;
        // Map each transition start event to a stream of:
        // start -> (success|error)
        var transEvents$ = this._globals.start$.pipe(switchMap(function (trans) {
            var event = function (evt) { return ({ evt: evt, trans: trans }); };
            var transStart$ = of(event('start'));
            var transResult = trans.promise.then(function () { return event('success'); }, function () { return event('error'); });
            var transFinish$ = from(transResult);
            return concat(transStart$, transFinish$);
        }));
        var withHostSref = function (childrenSrefs) {
            return childrenSrefs
                .concat(_this._hostUiSref)
                .filter(identity)
                .reduce(uniqR, []);
        };
        // Watch the @ContentChildren UISref[] components and get their target states
        this._srefs$ = new BehaviorSubject(withHostSref(this._srefs.toArray()));
        this._srefChangesSub = this._srefs.changes.subscribe(function (srefs) { return _this._srefs$.next(withHostSref(srefs)); });
        var targetStates$ = this._srefs$.pipe(switchMap(function (srefs) { return combineLatest(srefs.map(function (sref) { return sref.targetState$; })); }));
        // Calculate the status of each UISref based on the transition event.
        // Reduce the statuses (if multiple) by or-ing each flag.
        this._subscription = transEvents$
            .pipe(switchMap(function (evt) {
            return targetStates$.pipe(map(function (targets) {
                var statuses = targets.map(function (target) { return getSrefStatus(evt, target); });
                return statuses.reduce(mergeSrefStatus);
            }));
        }))
            .subscribe(this._setStatus.bind(this));
    };
    UISrefStatus.prototype.ngOnDestroy = function () {
        if (this._subscription)
            this._subscription.unsubscribe();
        if (this._srefChangesSub)
            this._srefChangesSub.unsubscribe();
        if (this._srefs$)
            this._srefs$.unsubscribe();
        this._subscription = this._srefChangesSub = this._srefs$ = undefined;
    };
    UISrefStatus.prototype._setStatus = function (status) {
        this.status = status;
        this.uiSrefStatus.emit(status);
    };
    __decorate([
        Output('uiSrefStatus'),
        __metadata("design:type", Object)
    ], UISrefStatus.prototype, "uiSrefStatus", void 0);
    __decorate([
        ContentChildren(UISref, { descendants: true }),
        __metadata("design:type", QueryList)
    ], UISrefStatus.prototype, "_srefs", void 0);
    UISrefStatus = __decorate([
        Directive({
            selector: '[uiSrefStatus],[uiSrefActive],[uiSrefActiveEq]',
            exportAs: 'uiSrefStatus',
        }),
        __param(0, Host()), __param(0, Self()), __param(0, Optional()),
        __metadata("design:paramtypes", [UISref, UIRouterGlobals])
    ], UISrefStatus);
    return UISrefStatus;
}());

/**
 * A directive that adds a CSS class when its associated `uiSref` link is active.
 *
 * ### Purpose
 *
 * This directive should be paired with one (or more) [[UISref]] directives.
 * It will apply a CSS class to its element when the state the `uiSref` targets is activated.
 *
 * This can be used to create navigation UI where the active link is highlighted.
 *
 * ### Selectors
 *
 * - `[uiSrefActive]`: When this selector is used, the class is added when the target state or any
 * child of the target state is active
 * - `[uiSrefActiveEq]`: When this selector is used, the class is added when the target state is
 * exactly active (the class is not added if a child of the target state is active).
 *
 * ### Inputs
 *
 * - `uiSrefActive`/`uiSrefActiveEq`: one or more CSS classes to add to the element, when the `uiSref` is active
 *
 * #### Example:
 * The anchor tag has the `active` class added when the `foo` state is active.
 * ```html
 * <a uiSref="foo" uiSrefActive="active">Foo</a>
 * ```
 *
 * ### Matching parameters
 *
 * If the `uiSref` includes parameters, the current state must be active, *and* the parameter values must match.
 *
 * #### Example:
 * The first anchor tag has the `active` class added when the `foo.bar` state is active and the `id` parameter
 * equals 25.
 * The second anchor tag has the `active` class added when the `foo.bar` state is active and the `id` parameter
 * equals 32.
 * ```html
 * <a uiSref="foo.bar" [uiParams]="{ id: 25 }" uiSrefActive="active">Bar #25</a>
 * <a uiSref="foo.bar" [uiParams]="{ id: 32 }" uiSrefActive="active">Bar #32</a>
 * ```
 *
 * #### Example:
 * A list of anchor tags are created for a list of `bar` objects.
 * An anchor tag will have the `active` class when `foo.bar` state is active and the `id` parameter matches
 * that object's `id`.
 * ```html
 * <li *ngFor="let bar of bars">
 *   <a uiSref="foo.bar" [uiParams]="{ id: bar.id }" uiSrefActive="active">Bar #{{ bar.id }}</a>
 * </li>
 * ```
 *
 * ### Multiple uiSrefs
 *
 * A single `uiSrefActive` can be used for multiple `uiSref` links.
 * This can be used to create (for example) a drop down navigation menu, where the menui is highlighted
 * if *any* of its inner links are active.
 *
 * The `uiSrefActive` should be placed on an ancestor element of the `uiSref` list.
 * If anyof the `uiSref` links are activated, the class will be added to the ancestor element.
 *
 * #### Example:
 * This is a dropdown nagivation menu for "Admin" states.
 * When any of `admin.users`, `admin.groups`, `admin.settings` are active, the `<li>` for the dropdown
 * has the `dropdown-child-active` class applied.
 * Additionally, the active anchor tag has the `active` class applied.
 * ```html
 * <ul class="dropdown-menu">
 *   <li uiSrefActive="dropdown-child-active" class="dropdown admin">
 *     Admin
 *     <ul>
 *       <li><a uiSref="admin.users" uiSrefActive="active">Users</a></li>
 *       <li><a uiSref="admin.groups" uiSrefActive="active">Groups</a></li>
 *       <li><a uiSref="admin.settings" uiSrefActive="active">Settings</a></li>
 *     </ul>
 *   </li>
 * </ul>
 * ```
 */
var UISrefActive = /** @class */ (function () {
    function UISrefActive(uiSrefStatus, rnd, host) {
        var _this = this;
        this._classes = [];
        this._classesEq = [];
        this._subscription = uiSrefStatus.uiSrefStatus.subscribe(function (next) {
            _this._classes.forEach(function (cls) {
                if (next.active) {
                    rnd.addClass(host.nativeElement, cls);
                }
                else {
                    rnd.removeClass(host.nativeElement, cls);
                }
            });
            _this._classesEq.forEach(function (cls) {
                if (next.exact) {
                    rnd.addClass(host.nativeElement, cls);
                }
                else {
                    rnd.removeClass(host.nativeElement, cls);
                }
            });
        });
    }
    Object.defineProperty(UISrefActive.prototype, "active", {
        set: function (val) {
            this._classes = val.split(/\s+/);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(UISrefActive.prototype, "activeEq", {
        set: function (val) {
            this._classesEq = val.split(/\s+/);
        },
        enumerable: true,
        configurable: true
    });
    UISrefActive.prototype.ngOnDestroy = function () {
        this._subscription.unsubscribe();
    };
    __decorate([
        Input('uiSrefActive'),
        __metadata("design:type", String),
        __metadata("design:paramtypes", [String])
    ], UISrefActive.prototype, "active", null);
    __decorate([
        Input('uiSrefActiveEq'),
        __metadata("design:type", String),
        __metadata("design:paramtypes", [String])
    ], UISrefActive.prototype, "activeEq", null);
    UISrefActive = __decorate([
        Directive({
            selector: '[uiSrefActive],[uiSrefActiveEq]',
        }),
        __param(2, Host()),
        __metadata("design:paramtypes", [UISrefStatus, Renderer2, ElementRef])
    ], UISrefActive);
    return UISrefActive;
}());

/**
 * The UI-Router for Angular directives:
 *
 * - [[UIView]]: A viewport for routed components
 * - [[UISref]]: A state ref to a target state; navigates when clicked
 * - [[UISrefActive]]: (and `UISrefActiveEq`) Adds a css class when a UISref's target state (or a child state) is active
 *
 * @ng2api
 * @preferred
 * @module directives
 */ /** */
/** @internalapi */
var _UIROUTER_DIRECTIVES = [UISref, AnchorUISref, UIView, UISrefActive, UISrefStatus];
/**
 * References to the UI-Router directive classes, for use within a @Component's `directives:` property
 * @deprecated use [[UIRouterModule]]
 * @internalapi
 */
var UIROUTER_DIRECTIVES = _UIROUTER_DIRECTIVES;

/** @hidden */ var UIROUTER_ROOT_MODULE = new InjectionToken('UIRouter Root Module');
/** @hidden */ var UIROUTER_MODULE_TOKEN = new InjectionToken('UIRouter Module');
/** @hidden */ var UIROUTER_STATES = new InjectionToken('UIRouter States');
// Delay angular bootstrap until first transition is successful, for SSR.
// See https://github.com/ui-router/angular/pull/127
function onTransitionReady(transitionService, root) {
    var mod = root[0];
    if (!mod || !mod.deferInitialRender) {
        return function () { return Promise.resolve(); };
    }
    return function () {
        return new Promise(function (resolve) {
            var hook = function (trans) {
                trans.promise.then(resolve, resolve);
            };
            transitionService.onStart({}, hook, { invokeLimit: 1 });
        });
    };
}
function makeRootProviders(module) {
    return [
        { provide: UIROUTER_ROOT_MODULE, useValue: module, multi: true },
        { provide: UIROUTER_MODULE_TOKEN, useValue: module, multi: true },
        { provide: ANALYZE_FOR_ENTRY_COMPONENTS, useValue: module.states || [], multi: true },
        {
            provide: APP_INITIALIZER,
            useFactory: onTransitionReady,
            deps: [TransitionService, UIROUTER_ROOT_MODULE],
            multi: true,
        },
    ];
}
function makeChildProviders(module) {
    return [
        { provide: UIROUTER_MODULE_TOKEN, useValue: module, multi: true },
        { provide: ANALYZE_FOR_ENTRY_COMPONENTS, useValue: module.states || [], multi: true },
    ];
}
function locationStrategy(useHash) {
    return { provide: LocationStrategy, useClass: useHash ? HashLocationStrategy : PathLocationStrategy };
}
/**
 * Creates UI-Router Modules
 *
 * This class has two static factory methods which create UIRouter Modules.
 * A UI-Router Module is an [Angular NgModule](https://angular.io/docs/ts/latest/guide/ngmodule.html)
 * with support for UI-Router.
 *
 * ### UIRouter Directives
 *
 * When a UI-Router Module is imported into a `NgModule`, that module's components
 * can use the UIRouter Directives such as [[UIView]], [[UISref]], [[UISrefActive]].
 *
 * ### State Definitions
 *
 * State definitions found in the `states:` property are provided to the Dependency Injector.
 * This enables UI-Router to automatically register the states with the [[StateRegistry]] at bootstrap (and during lazy load).
 *
 * ### Entry Components
 *
 * Any routed components are added as `entryComponents:` so they will get compiled.
 */
var UIRouterModule = /** @class */ (function () {
    function UIRouterModule() {
    }
    UIRouterModule_1 = UIRouterModule;
    /**
     * Creates a UI-Router Module for the root (bootstrapped) application module to import
     *
     * This factory function creates an [Angular NgModule](https://angular.io/docs/ts/latest/guide/ngmodule.html)
     * with UI-Router support.
     *
     * The `forRoot` module should be added to the `imports:` of the `NgModule` being bootstrapped.
     * An application should only create and import a single `NgModule` using `forRoot()`.
     * All other modules should be created using [[UIRouterModule.forChild]].
     *
     * Unlike `forChild`, an `NgModule` returned by this factory provides the [[UIRouter]] singleton object.
     * This factory also accepts root-level router configuration.
     * These are the only differences between `forRoot` and `forChild`.
     *
     * Example:
     * ```js
     * let routerConfig = {
     *   otherwise: '/home',
     *   states: [homeState, aboutState]
     * };
     *
     * @ NgModule({
     *   imports: [
     *     BrowserModule,
     *     UIRouterModule.forRoot(routerConfig),
     *     FeatureModule1
     *   ]
     * })
     * class MyRootAppModule {}
     *
     * browserPlatformDynamic.bootstrapModule(MyRootAppModule);
     * ```
     *
     * @param config declarative UI-Router configuration
     * @returns an `NgModule` which provides the [[UIRouter]] singleton instance
     */
    UIRouterModule.forRoot = function (config) {
        if (config === void 0) { config = {}; }
        return {
            ngModule: UIRouterModule_1,
            providers: __spread([
                _UIROUTER_INSTANCE_PROVIDERS,
                _UIROUTER_SERVICE_PROVIDERS,
                locationStrategy(config.useHash)
            ], makeRootProviders(config)),
        };
    };
    /**
     * Creates an `NgModule` for a UIRouter module
     *
     * This function creates an [Angular NgModule](https://angular.io/docs/ts/latest/guide/ngmodule.html)
     * with UI-Router support.
     *
     * #### Example:
     * ```js
     * var homeState = { name: 'home', url: '/home', component: Home };
     * var aboutState = { name: 'about', url: '/about', component: About };
     *
     * @ NgModule({
     *   imports: [
     *     UIRouterModule.forChild({ states: [ homeState, aboutState ] }),
     *     SharedModule,
     *   ],
     *   declarations: [ Home, About ],
     * })
     * export class AppModule {};
     * ```
     *
     * @param module UI-Router module options
     * @returns an `NgModule`
     */
    UIRouterModule.forChild = function (module) {
        if (module === void 0) { module = {}; }
        return {
            ngModule: UIRouterModule_1,
            providers: makeChildProviders(module),
        };
    };
    var UIRouterModule_1;
    UIRouterModule = UIRouterModule_1 = __decorate([
        NgModule({
            imports: [CommonModule],
            declarations: [_UIROUTER_DIRECTIVES],
            exports: [_UIROUTER_DIRECTIVES],
            entryComponents: [UIView],
        })
    ], UIRouterModule);
    return UIRouterModule;
}());

/** @ng2api @module core */
/**
 * Returns a function which lazy loads a nested module
 *
 * This is primarily used by the [[ng2LazyLoadBuilder]] when processing [[Ng2StateDeclaration.loadChildren]].
 *
 * It could also be used manually as a [[StateDeclaration.lazyLoad]] property to lazy load an `NgModule` and its state(s).
 *
 * #### Example:
 * Using `import()` and named export of `HomeModule`
 * ```js
 * declare var System;
 * var futureState = {
 *   name: 'home.**',
 *   url: '/home',
 *   lazyLoad: loadNgModule(() => import('./home/home.module').then(result => result.HomeModule))
 * }
 * ```
 *
 * #### Example:
 * Using a path (string) to the module
 * ```js
 * var futureState = {
 *   name: 'home.**',
 *   url: '/home',
 *   lazyLoad: loadNgModule('./home/home.module#HomeModule')
 * }
 * ```
 *
 *
 * @param moduleToLoad a path (string) to the NgModule to load.
 *    Or a function which loads the NgModule code which should
 *    return a reference to  the `NgModule` class being loaded (or a `Promise` for it).
 *
 * @returns A function which takes a transition, which:
 * - Gets the Injector (scoped properly for the destination state)
 * - Loads and creates the NgModule
 * - Finds the "replacement state" for the target state, and adds the new NgModule Injector to it (as a resolve)
 * - Returns the new states array
 */
function loadNgModule(moduleToLoad) {
    return function (transition, stateObject) {
        var ng2Injector = transition.injector().get(NATIVE_INJECTOR_TOKEN);
        var createModule = function (factory) { return factory.create(ng2Injector); };
        var applyModule = function (moduleRef) { return applyNgModule(transition, moduleRef, ng2Injector, stateObject); };
        return loadModuleFactory(moduleToLoad, ng2Injector)
            .then(createModule)
            .then(applyModule);
    };
}
/**
 * Returns the module factory that can be used to instantiate a module
 *
 * For strings this:
 * - Finds the correct NgModuleFactoryLoader
 * - Loads the new NgModuleFactory from the path string (async)
 *
 * For a Type<any> or Promise<Type<any>> this:
 * - Compiles the component type (if not running with AOT)
 * - Returns the NgModuleFactory resulting from compilation (or direct loading if using AOT) as a Promise
 *
 * @internalapi
 */
function loadModuleFactory(moduleToLoad, ng2Injector) {
    if (isString(moduleToLoad)) {
        return ng2Injector.get(NgModuleFactoryLoader).load(moduleToLoad);
    }
    var compiler = ng2Injector.get(Compiler);
    var unwrapEsModuleDefault = function (x) { return (x && x.__esModule && x['default'] ? x['default'] : x); };
    return Promise.resolve(moduleToLoad())
        .then(unwrapEsModuleDefault)
        .then(function (t) {
        if (t instanceof NgModuleFactory) {
            return t;
        }
        return compiler.compileModuleAsync(t);
    });
}
/**
 * Apply the UI-Router Modules found in the lazy loaded module.
 *
 * Apply the Lazy Loaded NgModule's newly created Injector to the right state in the state tree.
 *
 * Lazy loading uses a placeholder state which is removed (and replaced) after the module is loaded.
 * The NgModule should include a state with the same name as the placeholder.
 *
 * Find the *newly loaded state* with the same name as the *placeholder state*.
 * The NgModule's Injector (and ComponentFactoryResolver) will be added to that state.
 * The Injector/Factory are used when creating Components for the `replacement` state and all its children.
 *
 * @internalapi
 */
function applyNgModule(transition, ng2Module, parentInjector, lazyLoadState) {
    var injector = ng2Module.injector;
    var uiRouter = injector.get(UIRouter);
    var registry = uiRouter.stateRegistry;
    var originalName = lazyLoadState.name;
    var originalState = registry.get(originalName);
    // Check if it's a future state (ends with .**)
    var isFuture = /^(.*)\.\*\*$/.exec(originalName);
    // Final name (without the .**)
    var replacementName = isFuture && isFuture[1];
    var newRootModules = multiProviderParentChildDelta(parentInjector, injector, UIROUTER_ROOT_MODULE).reduce(uniqR, []);
    var newChildModules = multiProviderParentChildDelta(parentInjector, injector, UIROUTER_MODULE_TOKEN).reduce(uniqR, []);
    if (newRootModules.length) {
        console.log(newRootModules); // tslint:disable-line:no-console
        throw new Error('Lazy loaded modules should not contain a UIRouterModule.forRoot() module');
    }
    var newStateObjects = newChildModules
        .map(function (module) { return applyModuleConfig(uiRouter, injector, module); })
        .reduce(unnestR, [])
        .reduce(uniqR, []);
    if (isFuture) {
        var replacementState = registry.get(replacementName);
        if (!replacementState || replacementState === originalState) {
            throw new Error("The Future State named '" + originalName + "' lazy loaded an NgModule. " +
                ("The lazy loaded NgModule must have a state named '" + replacementName + "' ") +
                ("which replaces the (placeholder) '" + originalName + "' Future State. ") +
                ("Add a '" + replacementName + "' state to the lazy loaded NgModule ") +
                "using UIRouterModule.forChild({ states: CHILD_STATES }).");
        }
    }
    // Supply the newly loaded states with the Injector from the lazy loaded NgModule.
    // If a tree of states is lazy loaded, only add the injector to the root of the lazy loaded tree.
    // The children will get the injector by resolve inheritance.
    var newParentStates = newStateObjects.filter(function (state) { return !inArray(newStateObjects, state.parent); });
    // Add the Injector to the top of the lazy loaded state tree as a resolve
    newParentStates.forEach(function (state) { return state.resolvables.push(Resolvable.fromData(NATIVE_INJECTOR_TOKEN, injector)); });
    return {};
}
/**
 * Returns the new dependency injection values from the Child Injector
 *
 * When a DI token is defined as multi: true, the child injector
 * can add new values for the token.
 *
 * This function returns the values added by the child injector,  and excludes all values from the parent injector.
 *
 * @internalapi
 */
function multiProviderParentChildDelta(parent, child, token) {
    var childVals = child.get(token, []);
    var parentVals = parent.get(token, []);
    return childVals.filter(function (val) { return parentVals.indexOf(val) === -1; });
}

/**
 * This is a [[StateBuilder.builder]] function for ngModule lazy loading in Angular.
 *
 * When the [[StateBuilder]] builds a [[State]] object from a raw [[StateDeclaration]], this builder
 * decorates the `lazyLoad` property for states that have a [[Ng2StateDeclaration.ngModule]] declaration.
 *
 * If the state has a [[Ng2StateDeclaration.ngModule]], it will create a `lazyLoad` function
 * that in turn calls `loadNgModule(loadNgModuleFn)`.
 *
 * #### Example:
 * A state that has a `ngModule`
 * ```js
 * var decl = {
 *   ngModule: () => import('./childModule.ts')
 * }
 * ```
 * would build a state with a `lazyLoad` function like:
 * ```js
 * import { loadNgModule } from "@uirouter/angular";
 * var decl = {
 *   lazyLoad: loadNgModule(() => import('./childModule.ts')
 * }
 * ```
 *
 * If the state has both a `ngModule:` *and* a `lazyLoad`, then the `lazyLoad` is run first.
 *
 * #### Example:
 * ```js
 * var decl = {
 *   lazyLoad: () => import('third-party-library'),
 *   ngModule: () => import('./childModule.ts')
 * }
 * ```
 * would build a state with a `lazyLoad` function like:
 * ```js
 * import { loadNgModule } from "@uirouter/angular";
 * var decl = {
 *   lazyLoad: () => import('third-party-library')
 *       .then(() => loadNgModule(() => import('./childModule.ts'))
 * }
 * ```
 *
 */
function ng2LazyLoadBuilder(state, parent) {
    var loadNgModuleFn = state['loadChildren'];
    return loadNgModuleFn ? loadNgModule(loadNgModuleFn) : state.lazyLoad;
}

/** A `LocationServices` that delegates to the Angular LocationStrategy */
var Ng2LocationServices = /** @class */ (function (_super) {
    __extends(Ng2LocationServices, _super);
    function Ng2LocationServices(router, _locationStrategy, isBrowser) {
        var _this = _super.call(this, router, isBrowser) || this;
        _this._locationStrategy = _locationStrategy;
        _this._locationStrategy.onPopState(function (evt) {
            if (evt.type !== 'hashchange') {
                _this._listener(evt);
            }
        });
        return _this;
    }
    Ng2LocationServices.prototype._get = function () {
        return this._locationStrategy.path(true).replace(this._locationStrategy.getBaseHref().replace(/\/$/, ''), '');
    };
    Ng2LocationServices.prototype._set = function (state, title, url, replace) {
        var _a = parseUrl(url), path = _a.path, search = _a.search, hash = _a.hash;
        var urlWithHash = path + (hash ? '#' + hash : '');
        if (replace) {
            this._locationStrategy.replaceState(state, title, urlWithHash, search);
        }
        else {
            this._locationStrategy.pushState(state, title, urlWithHash, search);
        }
    };
    Ng2LocationServices.prototype.dispose = function (router) {
        _super.prototype.dispose.call(this, router);
    };
    return Ng2LocationServices;
}(BaseLocationServices));

/** @module ng2 */
var Ng2LocationConfig = /** @class */ (function (_super) {
    __extends(Ng2LocationConfig, _super);
    function Ng2LocationConfig(router, _locationStrategy) {
        var _this = _super.call(this, router, is(PathLocationStrategy)(_locationStrategy)) || this;
        _this._locationStrategy = _locationStrategy;
        return _this;
    }
    Ng2LocationConfig.prototype.baseHref = function (href) {
        return this._locationStrategy.getBaseHref();
    };
    return Ng2LocationConfig;
}(BrowserLocationConfig));

/**
 * # UI-Router for Angular (v2+)
 *
 * - [@uirouter/angular home page](https://ui-router.github.io/ng2)
 * - [tutorials](https://ui-router.github.io/tutorial/ng2/helloworld)
 * - [quick start repository](http://github.com/ui-router/quickstart-ng2)
 *
 * Getting started:
 *
 * - Use npm. Add a dependency on latest `@uirouter/angular`
 * - Import UI-Router classes directly from `"@uirouter/angular"`
 *
 * ```js
 * import {StateRegistry} from "@uirouter/angular";
 * ```
 *
 * - Create application states (as defined by [[Ng2StateDeclaration]]).
 *
 * ```js
 * export let state1: Ng2StateDeclaration = {
 *   name: 'state1',
 *   component: State1Component,
 *   url: '/one'
 * }
 *
 * export let state2: Ng2StateDeclaration = {
 *   name: 'state2',
 *   component: State2Component,
 *   url: '/two'
 * }
 * ```
 *
 * - Import a [[UIRouterModule.forChild]] module into your feature `NgModule`s.
 *
 * ```js
 * @ NgModule({
 *   imports: [
 *     SharedModule,
 *     UIRouterModule.forChild({ states: [state1, state2 ] })
 *   ],
 *   declarations: [
 *     State1Component,
 *     State2Component,
 *   ]
 * })
 * export class MyFeatureModule {}
 * ```
 *
 * - Import a [[UIRouterModule.forRoot]] module into your application root `NgModule`
 * - Either bootstrap a [[UIView]] component, or add a `<ui-view></ui-view>` viewport to your root component.
 *
 * ```js
 * @ NgModule({
 *   imports: [
 *     BrowserModule,
 *     UIRouterModule.forRoot({ states: [ homeState ] }),
 *     MyFeatureModule,
 *   ],
 *   declarations: [
 *     HomeComponent
 *   ]
 *   bootstrap: [ UIView ]
 * })
 * class RootAppModule {}
 *
 * browserPlatformDynamic.bootstrapModule(RootAppModule);
 * ```
 *
 * - Optionally specify a configuration class [[ChildModule.configClass]] for any module
 * to perform any router configuration during bootstrap or lazyload.
 * Pass the class to [[UIRouterModule.forRoot]] or [[UIRouterModule.forChild]].
 *
 * ```js
 * import {UIRouter} from "@uirouter/angular";
 *
 * @ Injectable()
 * export class MyUIRouterConfig {
 *   // Constructor is injectable
 *   constructor(uiRouter: UIRouter) {
 *     uiRouter.urlMatcherFactory.type('datetime', myDateTimeParamType);
 *   }
 * }
 * ```
 *
 * @preferred @module ng2
 */
/**
 * This is a factory function for a UIRouter instance
 *
 * Creates a UIRouter instance and configures it for Angular, then invokes router bootstrap.
 * This function is used as an Angular `useFactory` Provider.
 */
function uiRouterFactory(locationStrategy, rootModules, modules, injector) {
    if (rootModules.length !== 1) {
        throw new Error("Exactly one UIRouterModule.forRoot() should be in the bootstrapped app module's imports: []");
    }
    // ----------------- Create router -----------------
    // Create a new ng2 UIRouter and configure it for ng2
    var router = new UIRouter();
    // Add RxJS plugin
    router.plugin(UIRouterRx);
    // Add $q-like and $injector-like service APIs
    router.plugin(servicesPlugin);
    // ----------------- Monkey Patches ----------------
    // Monkey patch the services.$injector to use the root ng2 Injector
    services.$injector.get = injector.get.bind(injector);
    // ----------------- Configure for ng2 -------------
    router.locationService = new Ng2LocationServices(router, locationStrategy, isPlatformBrowser(injector.get(PLATFORM_ID)));
    router.locationConfig = new Ng2LocationConfig(router, locationStrategy);
    // Apply ng2 ui-view handling code
    var viewConfigFactory = function (path, config) { return new Ng2ViewConfig(path, config); };
    router.viewService._pluginapi._viewConfigFactory('ng2', viewConfigFactory);
    // Apply statebuilder decorator for ng2 NgModule registration
    var registry = router.stateRegistry;
    registry.decorator('views', ng2ViewsBuilder);
    registry.decorator('lazyLoad', ng2LazyLoadBuilder);
    // Prep the tree of NgModule by placing the root NgModule's Injector on the root state.
    var ng2InjectorResolvable = Resolvable.fromData(NATIVE_INJECTOR_TOKEN, injector);
    registry.root().resolvables.push(ng2InjectorResolvable);
    // Auto-flush the parameter type queue
    router.urlMatcherFactory.$get();
    // ----------------- Initialize router -------------
    rootModules.forEach(function (moduleConfig) { return applyRootModuleConfig(router, injector, moduleConfig); });
    modules.forEach(function (moduleConfig) { return applyModuleConfig(router, injector, moduleConfig); });
    return router;
}
// Start monitoring the URL when the app starts
function appInitializer(router) {
    return function () {
        if (!router.urlRouter.interceptDeferred) {
            router.urlService.listen();
            router.urlService.sync();
        }
    };
}
function parentUIViewInjectFactory(r) {
    return { fqn: null, context: r.root() };
}
var _UIROUTER_INSTANCE_PROVIDERS = [
    {
        provide: UIRouter,
        useFactory: uiRouterFactory,
        deps: [LocationStrategy, UIROUTER_ROOT_MODULE, UIROUTER_MODULE_TOKEN, Injector],
    },
    { provide: UIView.PARENT_INJECT, useFactory: parentUIViewInjectFactory, deps: [StateRegistry] },
    { provide: APP_INITIALIZER, useFactory: appInitializer, deps: [UIRouter], multi: true },
];
function fnStateService(r) {
    return r.stateService;
}
function fnTransitionService(r) {
    return r.transitionService;
}
function fnUrlMatcherFactory(r) {
    return r.urlMatcherFactory;
}
function fnUrlRouter(r) {
    return r.urlRouter;
}
function fnUrlService(r) {
    return r.urlService;
}
function fnViewService(r) {
    return r.viewService;
}
function fnStateRegistry(r) {
    return r.stateRegistry;
}
function fnGlobals(r) {
    return r.globals;
}
var _UIROUTER_SERVICE_PROVIDERS = [
    { provide: StateService, useFactory: fnStateService, deps: [UIRouter] },
    { provide: TransitionService, useFactory: fnTransitionService, deps: [UIRouter] },
    { provide: UrlMatcherFactory, useFactory: fnUrlMatcherFactory, deps: [UIRouter] },
    { provide: UrlRouter, useFactory: fnUrlRouter, deps: [UIRouter] },
    { provide: UrlService, useFactory: fnUrlService, deps: [UIRouter] },
    { provide: ViewService, useFactory: fnViewService, deps: [UIRouter] },
    { provide: StateRegistry, useFactory: fnStateRegistry, deps: [UIRouter] },
    { provide: UIRouterGlobals, useFactory: fnGlobals, deps: [UIRouter] },
];
/**
 * The UI-Router providers, for use in your application bootstrap
 *
 * @deprecated use [[UIRouterModule.forRoot]]
 */
var UIROUTER_PROVIDERS = _UIROUTER_INSTANCE_PROVIDERS.concat(_UIROUTER_SERVICE_PROVIDERS);

export { AnchorUISref, Ng2ViewConfig, UIROUTER_DIRECTIVES, UIROUTER_MODULE_TOKEN, UIROUTER_PROVIDERS, UIROUTER_ROOT_MODULE, UIROUTER_STATES, UIRouterModule, UISref, UISrefActive, UISrefStatus, UIView, _UIROUTER_DIRECTIVES, _UIROUTER_INSTANCE_PROVIDERS, _UIROUTER_SERVICE_PROVIDERS, appInitializer, applyModuleConfig, applyNgModule, applyRootModuleConfig, fnGlobals, fnStateRegistry, fnStateService, fnTransitionService, fnUrlMatcherFactory, fnUrlRouter, fnUrlService, fnViewService, loadModuleFactory, loadNgModule, locationStrategy, makeChildProviders, makeRootProviders, multiProviderParentChildDelta, ng2LazyLoadBuilder, ng2ViewsBuilder, onTransitionReady, parentUIViewInjectFactory, uiRouterFactory, ɵ0 };
