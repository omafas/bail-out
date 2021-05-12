/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnXDCR = (function (Rx) {
  "use strict";

  MnXDCRService.annotations = [
    new ng.core.Injectable()
  ];

  MnXDCRService.parameters = [
    ng.common.http.HttpClient
  ];

  MnXDCRService.prototype.deleteRemoteClusters = deleteRemoteClusters;
  MnXDCRService.prototype.deleteCancelXDCR = deleteCancelXDCR;
  MnXDCRService.prototype.getSettingsReplications = getSettingsReplications;
  MnXDCRService.prototype.postSettingsReplications = postSettingsReplications;
  MnXDCRService.prototype.postCreateReplication = postCreateReplication;
  MnXDCRService.prototype.getRemoteClusters = getRemoteClusters;
  MnXDCRService.prototype.postRemoteClusters = postRemoteClusters;
  MnXDCRService.prototype.createGetSettingsReplicationsPipe = createGetSettingsReplicationsPipe;
  MnXDCRService.prototype.prepareReplicationSettigns = prepareReplicationSettigns;

  return MnXDCRService;

  function MnXDCRService(http) {
    this.http = http;

    this.stream = {};

    this.stream.updateRemoteClusters =
      new Rx.BehaviorSubject();

    this.stream.deleteRemoteClusters =
      new mn.core.MnPostHttp(this.deleteRemoteClusters.bind(this))
      .addSuccess()
      .addError();

    this.stream.deleteCancelXDCR =
      new mn.core.MnPostHttp(this.deleteCancelXDCR.bind(this))
      .addSuccess()
      .addError();

    this.stream.getSettingsReplications = this.createGetSettingsReplicationsPipe();

    this.stream.postSettingsReplications =
      new mn.core.MnPostHttp(this.postSettingsReplications(false).bind(this))
      .addSuccess()
      .addError();

    this.stream.postPausePlayReplication =
      new mn.core.MnPostHttp(this.postSettingsReplications(false).bind(this))
      .addSuccess()
      .addError();

    this.stream.postSettingsReplicationsValidation =
      new mn.core.MnPostHttp(this.postSettingsReplications(true).bind(this))
      .addSuccess()
      .addError();

    this.stream.postCreateReplication =
      new mn.core.MnPostHttp(this.postCreateReplication.bind(this))
      .addSuccess()
      .addError(Rx.operators.map(function (error) {
        return (typeof error == "string") ? {_: error} : error;
      }));

    this.stream.postRemoteClusters =
      new mn.core.MnPostHttp(this.postRemoteClusters.bind(this))
      .addSuccess()
      .addError();

    this.stream.getRemoteClusters = Rx.combineLatest(
      Rx.timer(0, 10000),
      this.stream.updateRemoteClusters
    ).pipe(Rx.operators.switchMap(this.getRemoteClusters.bind(this)),
           mn.core.rxOperatorsShareReplay(1));

    this.stream.getRemoteClustersFiltered = this.stream.getRemoteClusters
      .pipe(Rx.operators.map(R.pipe(R.filter(R.propEq('deleted', false)),
                                    R.sortBy(R.prop('name')))),
            mn.core.rxOperatorsShareReplay(1));

    this.stream.getRemoteClustersByUUID =
      this.stream.getRemoteClusters.pipe(Rx.operators.map(R.groupBy(R.prop("uuid"))),
                                         mn.core.rxOperatorsShareReplay(1));


  }

  function prepareReplicationSettigns(source) {
    var settings = Object.assign({}, this.form.group.value);
    if (!source[0] || !source[1] || settings.type == "capi") {
      delete settings.compressionType;
    }
    if (!source[0] || settings.type !== "xmem") {
      delete settings.networkUsageLimit;
    }
    settings.replicationType = "continuous";
    return settings;
  }

  function createGetSettingsReplicationsPipe(id) {
    return (new Rx.BehaviorSubject(id)).pipe(
      Rx.operators.switchMap(this.getSettingsReplications.bind(this)),
      mn.core.rxOperatorsShareReplay(1));
  }

  function deleteRemoteClusters(name) {
    return this.http.delete('/pools/default/remoteClusters/' + encodeURIComponent(name));
  }

  function deleteCancelXDCR(id) {
    return this.http.delete('/controller/cancelXDCR/' + encodeURIComponent(id));
  }

  function getSettingsReplications(id) {
    return this.http.get("/settings/replications" +
                         (id ? ("/" + encodeURIComponent(id)) : ""));
  }

  function postSettingsReplications(validate) {
    return function (source) {
      return this.http.post("/settings/replications" +
                            (source[0] ? ("/" + encodeURIComponent(source[0])) : ""),
                            source[0] ? source[1] : source,
                            {params: {"just_validate": validate ? 1 : 0}});
    }
  }

  function postCreateReplication(data) {
    return this.http.post("/controller/createReplication", data);
  }

  function getRemoteClusters() {
    return this.http.get("/pools/default/remoteClusters");
  }

  function postRemoteClusters(source) {
    var cluster = source[0];
    var name = source[1];
    var re;
    var result;
    if (cluster.hostname) {
      re = /^\[?([^\]]+)\]?:(\d+)$/; // ipv4/ipv6/hostname + port
      result = re.exec(cluster.hostname);
      if (!result) {
        cluster.hostname += ":8091";
      }
    }
    if (!cluster.demandEncryption) {
      delete cluster.certificate;
      delete cluster.demandEncryption;
      delete cluster.encryptionType;
      delete cluster.clientCertificate;
      delete cluster.clientKey;
    }
    delete cluster.secureType;
    return this.http.post('/pools/default/remoteClusters' + (name ? ("/" + encodeURIComponent(name)) : ""), cluster);
  }

})(window.rxjs);
