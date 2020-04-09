describe('mnHttp', function () {
  var callback;

  beforeEach(function () {
    module('mnPendingQueryKeeper');
    module('mnHttp');
    callback = jasmine.createSpy('callback');
  });

  it('should have the right methods', inject(function (mnHttpInterceptor) {
    var methods = ['request', 'response'];
    angular.forEach(methods, function (method) {
      expect(mnHttpInterceptor[method]).toEqual(jasmine.any(Function));
    });
    expect(mnHttpInterceptor).toEqual(jasmine.any(Object));
  }));

  it('should pass correct configuration into $http', function () {
    //in this test we check that:
    //1. short method merges configuration correctly
    //2. 'httpConfig' property reaches $http.
    //3. canceler ('timeout') is added to each type of request
    //4. if data has type string it shouldn't be serialized
    //5. if isNotForm is true then application/x-www-form-urlencoded header should not be passed into http
    //6. mnHttp property does not reach $http
    //7. configuration remained intact for mnPendingQueryKeeper.push

    inject(function ($http, $q, mnHttpInterceptor, mnPendingQueryKeeper) {
      spyOn(mnPendingQueryKeeper, "push");
      expect(mnHttpInterceptor.request({
        url: '/url',
        method: "GET",
        httpConfig: "httpConfig",
        mnHttp: {
          deleteMe: true
        }
      })).toEqual({
        httpConfig: 'httpConfig',
        method: 'GET',
        url: '/url',
        clear: jasmine.any(Function),
        timeout: $q.defer().promise
      });
      expect(mnPendingQueryKeeper.push).toHaveBeenCalledWith({
        config: {
          url: '/url',
          method: 'GET',
          httpConfig: 'httpConfig',
          mnHttp: {
            deleteMe: true
          }
        },
        canceler: jasmine.any(Function),
        group: undefined
      });
      expect(mnHttpInterceptor.request({
        url: '/url',
        method: "POST",
        data: {
          data: "data"
        },
        httpConfig: "httpConfig",
        mnHttp: {
          deleteMe: true
        }
      })).toEqual({
        httpConfig: 'httpConfig',
        method: 'POST',
        url: '/url',
        data: 'data=data',
        headers: {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
        timeout: $q.defer().promise,
        clear: jasmine.any(Function)
      });
      expect(mnHttpInterceptor.request({
        url: '/url',
        method: "PUT",
        data: "doNotSerializeMe",
        httpConfig: "httpConfig",
        mnHttp: {
          isNotForm: true
        }
      })).toEqual({
        httpConfig: 'httpConfig',
        method: 'PUT',
        url: '/url',
        headers: {},
        data: 'doNotSerializeMe',
        timeout: $q.defer().promise,
        clear: jasmine.any(Function)
      });
    });
  });

  it("should cancel previous post request in case new with same configuration and flag cancelPrevious is coming", inject(function ($http, $httpBackend, $rootScope, mnPendingQueryKeeper) {
    $httpBackend.expect('POST', '/url').respond(200);
    $httpBackend.expect('POST', '/url').respond(200);

    spyOn(mnPendingQueryKeeper, "getQueryInFly").and.callThrough();
    $http.post("/url", undefined, {mnHttp: {cancelPrevious: true}}).catch(function (response) {
      expect(response.status).toBe(-1);
      callback();
    });
    $http.post("/url", undefined, {mnHttp: {cancelPrevious: true}});
    $httpBackend.flush();

    expect(mnPendingQueryKeeper.getQueryInFly.calls.count()).toBe(2);
    expect(callback).toHaveBeenCalled();
    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  }));

  it("should cancel request by timeout in case timeout was passed", inject(function (mnHttpInterceptor, $timeout) {
    var config = mnHttpInterceptor.request({
      url: '/url',
      method: "GET",
      timeout: 10000
    });

    config.timeout.then(function (reason) {
      expect(reason).toBe("timeout");
      callback();
    });

    $timeout.flush(10000);

    expect(callback).toHaveBeenCalled();
  }));

  it("should be cleared properly", inject(function ($http, $timeout, $httpBackend, mnPendingQueryKeeper) {
    //in this test we check that protective logic (isCleared) works correctly in case we cancel previous query
    //without protective logic timeout.cancel and removeQueryInFly will be called 3 times
    spyOn(mnPendingQueryKeeper, "removeQueryInFly").and.callThrough();
    $httpBackend.expect('POST', '/url').respond(200);
    $httpBackend.expect('POST', '/url').respond(200);

    spyOn($timeout, "cancel").and.callThrough();
    $http.post("/url", undefined, {timeout: 10000, mnHttp: {cancelPrevious: true}});
    $http.post("/url", undefined, {timeout: 10000, mnHttp: {cancelPrevious: true}});
    $httpBackend.flush();

    expect($timeout.cancel.calls.count()).toBe(2);
    expect(mnPendingQueryKeeper.removeQueryInFly.calls.count()).toBe(2);

    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  }));

  it("should not be intercepted if appropriate flag was passed or if url has extansion .html", inject(function ($http, $timeout, $httpBackend, mnPendingQueryKeeper) {
    $httpBackend.expect('POST', '/url.html').respond(200);
    $httpBackend.expect('POST', '/url').respond(200);

    spyOn(mnPendingQueryKeeper, "push").and.callThrough();

    $http.post("/url.html", undefined, {});
    $http.post("/url", undefined, {doNotIntercept: true});

    $httpBackend.flush();
    expect(mnPendingQueryKeeper.push).not.toHaveBeenCalled();

    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  }));

});
