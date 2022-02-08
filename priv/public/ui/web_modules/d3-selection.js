import { f as creator, S as Selection, r as root } from './common/index-e88ffd88.js';
export { f as creator, c as customEvent, e as event, m as matcher, n as namespace, g as namespaces, b as selection, s as selector, a as selectorAll, d as style, h as window } from './common/index-e88ffd88.js';
import { s as select, a as sourceEvent, p as point } from './common/touch-775b0bb4.js';
export { p as clientPoint, m as mouse, s as select, t as touch } from './common/touch-775b0bb4.js';

function create(name) {
  return select(creator(name).call(document.documentElement));
}

var nextId = 0;

function local() {
  return new Local;
}

function Local() {
  this._ = "@" + (++nextId).toString(36);
}

Local.prototype = local.prototype = {
  constructor: Local,
  get: function(node) {
    var id = this._;
    while (!(id in node)) if (!(node = node.parentNode)) return;
    return node[id];
  },
  set: function(node, value) {
    return node[this._] = value;
  },
  remove: function(node) {
    return this._ in node && delete node[this._];
  },
  toString: function() {
    return this._;
  }
};

function selectAll(selector) {
  return typeof selector === "string"
      ? new Selection([document.querySelectorAll(selector)], [document.documentElement])
      : new Selection([selector == null ? [] : selector], root);
}

function touches(node, touches) {
  if (touches == null) touches = sourceEvent().touches;

  for (var i = 0, n = touches ? touches.length : 0, points = new Array(n); i < n; ++i) {
    points[i] = point(node, touches[i]);
  }

  return points;
}

export { create, local, selectAll, touches };
