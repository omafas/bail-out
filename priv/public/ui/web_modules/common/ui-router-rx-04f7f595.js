import { m as mergeMap, e as map } from './mergeMap-64c6f393.js';
import { R as ReplaySubject } from './ReplaySubject-8316d9c1.js';
import { f as filter } from './filter-d76a729c.js';

/** @module rx */
/** Augments UIRouterGlobals with observables for transition starts, successful transitions, and state parameters */
var UIRouterRx = /** @class */ (function () {
    function UIRouterRx(router) {
        this.name = '@uirouter/rx';
        this.deregisterFns = [];
        var start$ = new ReplaySubject(1);
        var success$ = start$.pipe(mergeMap(function (t) { return t.promise.then(function () { return t; }, function () { return null; }); }), filter(function (t) { return !!t; }));
        var params$ = success$.pipe(map(function (transition) { return transition.params(); }));
        var states$ = new ReplaySubject(1);
        function onStatesChangedEvent(event, states) {
            var changeEvent = {
                currentStates: router.stateRegistry.get(),
                registered: [],
                deregistered: [],
            };
            if (event)
                changeEvent[event] = states;
            states$.next(changeEvent);
        }
        this.deregisterFns.push(router.transitionService.onStart({}, function (transition) { return start$.next(transition); }));
        this.deregisterFns.push(router.stateRegistry.onStatesChanged(onStatesChangedEvent));
        onStatesChangedEvent(null, null);
        Object.assign(router.globals, { start$: start$, success$: success$, params$: params$, states$: states$ });
    }
    UIRouterRx.prototype.dispose = function () {
        this.deregisterFns.forEach(function (deregisterFn) { return deregisterFn(); });
        this.deregisterFns = [];
    };
    return UIRouterRx;
}());
var UIRouterRxPlugin = UIRouterRx;

export { UIRouterRx as U, UIRouterRxPlugin as a };
