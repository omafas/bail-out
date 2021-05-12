function n(n,r){return n=+n,r=+r,function(t){return Math.round(n*(1-t)+r*t)}}function r(n,r){for(var t=0,a=r.length-1,o=r[0],u=new Array(a<0?0:a);t<a;)u[t]=n(o,o=r[++t]);return function(n){var r=Math.max(0,Math.min(a-1,Math.floor(n*=a)));return u[r](n-r)}}export{n as i,r as p};
//# sourceMappingURL=piecewise-ef887050.js.map
