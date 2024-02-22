import '../common/tslib.es6-c4a4947b.js';
import { O as Observable } from '../common/mergeMap-64c6f393.js';
import '../common/Notification-9e07e457.js';
import '../common/ReplaySubject-8316d9c1.js';
import { o as of } from '../common/filter-d76a729c.js';
import '../common/ArgumentOutOfRangeError-91c779f5.js';
import '../common/EmptyError-a9e17542.js';
import { s as shareReplay, f as first } from '../common/shareReplay-5c54bf83.js';
import '../common/take-7bfdafe5.js';
export { U as UIRouterRx, a as UIRouterRxPlugin } from '../common/ui-router-rx-04f7f595.js';

/**
 * Determines the unwrapping behavior of asynchronous resolve values.
 *
 *   - When an Observable is returned from the resolveFn, wait until the Observable emits at least one item.
 *     If any other value will be converted to an Observable that emits such value.
 *   - The Observable item will not be unwrapped.
 *   - The Observable stream itself will be provided when the resolve is injected or bound elsewhere.
 *
 * #### Example:
 *
 * The `Transition` will wait for the `main.home` resolve observables to emit their first value.
 * Promises will be unwrapped and returned as observables before being provided to components.
 * ```js
 * var mainState = {
 *   name: 'main',
 *   resolve: mainResolves, // defined elsewhere
 *   resolvePolicy: { async: RXWAIT },
 * }
 * ```
 */
function RXWAIT(resolveFnValue) {
    if (!(resolveFnValue instanceof Observable)) {
        resolveFnValue = of(resolveFnValue);
    }
    var data$ = resolveFnValue.pipe(shareReplay(1));
    return data$
        .pipe(first())
        .toPromise()
        .then(function () {
        return data$;
    });
}

export { RXWAIT };
