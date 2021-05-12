/*! *****************************************************************************
Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at http://www.apache.org/licenses/LICENSE-2.0

THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
MERCHANTABLITY OR NON-INFRINGEMENT.

See the Apache Version 2.0 License for specific language governing permissions
and limitations under the License.
***************************************************************************** */
var t=function(e,r){return(t=Object.setPrototypeOf||{__proto__:[]}instanceof Array&&function(t,e){t.__proto__=e}||function(t,e){for(var r in e)e.hasOwnProperty(r)&&(t[r]=e[r])})(e,r)};function e(e,r){function n(){this.constructor=e}t(e,r),e.prototype=null===r?Object.create(r):(n.prototype=r.prototype,new n)}var r=function(){return(r=Object.assign||function(t){for(var e,r=1,n=arguments.length;r<n;r++)for(var o in e=arguments[r])Object.prototype.hasOwnProperty.call(e,o)&&(t[o]=e[o]);return t}).apply(this,arguments)};function n(t,e,r,n){var o,f=arguments.length,c=f<3?e:null===n?n=Object.getOwnPropertyDescriptor(e,r):n;if("object"==typeof Reflect&&"function"==typeof Reflect.decorate)c=Reflect.decorate(t,e,r,n);else for(var a=t.length-1;a>=0;a--)(o=t[a])&&(c=(f<3?o(c):f>3?o(e,r,c):o(e,r))||c);return f>3&&c&&Object.defineProperty(e,r,c),c}function o(t,e){return function(r,n){e(r,n,t)}}function f(t,e){if("object"==typeof Reflect&&"function"==typeof Reflect.metadata)return Reflect.metadata(t,e)}function c(t){var e="function"==typeof Symbol&&Symbol.iterator,r=e&&t[e],n=0;if(r)return r.call(t);if(t&&"number"==typeof t.length)return{next:function(){return t&&n>=t.length&&(t=void 0),{value:t&&t[n++],done:!t}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function a(t,e){var r="function"==typeof Symbol&&t[Symbol.iterator];if(!r)return t;var n,o,f=r.call(t),c=[];try{for(;(void 0===e||e-- >0)&&!(n=f.next()).done;)c.push(n.value)}catch(t){o={error:t}}finally{try{n&&!n.done&&(r=f.return)&&r.call(f)}finally{if(o)throw o.error}}return c}function i(){for(var t=[],e=0;e<arguments.length;e++)t=t.concat(a(arguments[e]));return t}export{n as _,f as a,e as b,o as c,a as d,c as e,r as f,i as g};
//# sourceMappingURL=tslib.es6-c4a4947b.js.map
