/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {UIRouter} from '@uirouter/angular';
import {filter, map, distinctUntilChanged, takeUntil} from 'rxjs/operators';
import {combineLatest, merge, pipe} from 'rxjs';
import {ClipboardService} from 'ngx-clipboard';

import {MnLifeCycleHooksToStream} from './mn.core.js';

import {MnAlerts} from './ajs.upgraded.providers.js';
import {MnFormService} from './mn.form.service.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnPoolsService} from './mn.pools.service.js';
import template from "./mn.security.saml.html";
import {clone} from 'ramda';

export {MnSecuritySamlComponent};

class MnSecuritySamlComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnHelperService,
    MnSecurityService,
    MnPermissions,
    MnPoolsService,
    MnAlerts,

    UIRouter,
    ClipboardService
  ]}

  constructor(mnFormService, mnHelperService, mnSecurityService, mnPermissions, mnPoolsService, mnAlerts, uiRouter, clipboardService) {
    super();

    this.mnHelperService = mnHelperService;
    this.uiRouter = uiRouter;
    this.mnAlerts = mnAlerts;

    this.getSaml = mnSecurityService.stream.getSaml;
    this.postSaml = mnSecurityService.stream.postSaml;
    this.postSamlValidation = mnSecurityService.stream.postSamlValidation;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.permissionsAdminSecurityWrite = mnPermissions.stream
      .pipe(map(permissions => permissions.cluster.admin.security.external.write));

    clipboardService.copyResponse$
      .pipe(filter(response => response.isSuccess),
            takeUntil(this.mnOnDestroy))
      .subscribe(() =>
        this.mnAlerts.formatAndSetAlerts('URL copied successfully!', 'success', 2500));

    clipboardService.copyResponse$
      .pipe(filter(response => !response.isSuccess),
            takeUntil(this.mnOnDestroy))
      .subscribe(() =>
        this.mnAlerts.formatAndSetAlerts('Unable to copy URL!', 'error', 2500));

    this.form = mnFormService.create(this);
    this.form
      .setFormGroup({
         authnNameIDFormat: null,
         enabled: null,
         groupsAttribute: null,
         groupsAttributeSep: null,
         groupsFilterRE: null,
         idpAuthnBinding: null,
         idpLogoutBinding: null,
         idpMetadata: null, // xml format
         idpMetadataURL: null,
         idpMetadataConnectAddressFamily: null,
         idpMetadataHttpTimeoutMs: null,
         idpMetadataOrigin: null, // Values: upload, http_one_time, http
         idpMetadataRefreshIntervalS: null,
         idpMetadataTLSCAs: null,
         idpMetadataTLSExtraOpts: null,
         idpMetadataTLSSNI: null,
         idpMetadataTLSVerifyPeer: null,
         idpSignsMetadata: null,
         rolesAttribute: null,
         rolesAttributeSep: null,
         rolesFilterRE: null,
         singleLogoutEnabled: null,
         spAssertionDupeCheck: null,
         spBaseURLScheme: null,
         spBaseURLType: null,
         spCertificate: null,
         spChain: null,
         spConsumeURL: null,
         spContactEmail: null,
         spContactName: null,
         spCustomBaseURL: null,
         spEntityId: null,
         spKey: null,
         spLogoutURL: null,
         spMetadataCacheDuration: null,
         spMetadataURL: null,
         spOrgDisplayName: null,
         spOrgName: null,
         spOrgURL: null,
         spSessionExpire: null,
         spSignMetadata: null,
         spSignRequests: null,
         spVerifyIssuer: null,
         spTrustedFingerprints: null,
         spTrustedFingerprintsUsage: null, // Values: everything, metadataOnly, metadataInitialOnly
         spVerifyAssertionEnvelopSig: null,
         spVerifyAssertionSig: null,
         spVerifyLogoutReqSig: null,
         spVerifyRecipient: null, // Values: consumeURL, custom, false
         spVerifyRecipientValue: null,
         usernameAttribute: null,

         // FE only form controls.
         idpMetadataRefreshIntervalSFlag: null,
         spVerifyRecipientFlag: null,
         usernameAttributeFlag: null,
         groupsAttributeFlag: null,
         rolesAttributeFlag: null,
         spAssertionDupeCheckFlag: null,
         spTrustedFingerprintsUsageMetadata: null,
         spTrustedFingerprintsUsageEverything: null,
      })
      .setUnpackPipe(pipe(map(this.unpackGetSaml.bind(this))))
      .setPackPipe(pipe(map(this.packPostSaml.bind(this))))
      .setSource(this.getSaml)
      .setPostRequest(this.postSaml)
      .setValidation(this.postSamlValidation, this.permissionsAdminSecurityWrite)
      .showGlobalSpinner()
      .successMessage("SAML settings saved successfully!")
      .clearErrors()
      .success(() => {
        this.form.setSource(this.getSaml);
      });

    this.form.group.disable();

    this.httpError = merge(this.postSaml.error, this.postSamlValidation.error);

    // Possible to receive a 500 error in the form of a single value array.
    this.postSamlGlobalError = this.httpError.pipe(
        map((error) => {
            if (error && Array.isArray(error)) {
                return error[0];
            }
        }));

    this.cancel = this.cancel.bind(this);

    this.enabled = this.form.group.get("enabled").valueChanges.pipe(distinctUntilChanged());

    // Toggle Streams.
    this.showServiceProviderMetaData = this.mnHelperService.createToggle();
    this.showKeyAndCertificates = this.mnHelperService.createToggle();
    this.showIdentityProviderMetaData = this.mnHelperService.createToggle();
    this.showTrustedFingerprints = this.mnHelperService.createToggle();
    this.showSingleSignOn = this.mnHelperService.createToggle();
    this.showAdvanced = this.mnHelperService.createToggle();

    // Error Streams.
    this.showServiceProviderMetaDataHasErrors =
      combineLatest(this.enabled, this.httpError)
      .pipe(filter(([_, errors]) => !!errors),
            map((this.showServiceProviderMetaDataHasErrors.bind(this))));

    this.showKeyAndCertificatesHasErrors =
      combineLatest(this.enabled, this.httpError)
      .pipe(filter(([_, errors]) => !!errors),
            map((this.showKeyAndCertificatesHasErrors.bind(this))));

    this.showIdentityProviderMetaDataHasErrors =
      combineLatest(this.enabled, this.httpError)
      .pipe(filter(([_, errors]) => !!errors),
            map((this.showIdentityProviderMetaDataHasErrors.bind(this))));

    this.showTrustedFingerprintsHasErrors =
      combineLatest(this.enabled, this.httpError)
      .pipe(filter(([_, errors]) => !!errors),
            map((this.showTrustedFingerprintsHasErrors.bind(this))));

    this.showSingleSignOnHasErrors =
      combineLatest(this.enabled, this.httpError)
      .pipe(filter(([_, errors]) => !!errors),
            map((this.showSingleSignOnHasErrors.bind(this))));

    this.showAdvancedHasErrors =
      combineLatest(this.enabled, this.httpError)
      .pipe(filter(([_, errors]) => !!errors),
            map((this.showAdvancedHasErrors.bind(this))));

    // Disabled Streams.
    combineLatest(
      this.enabled,
      this.permissionsAdminSecurityWrite
    )
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableForm.bind(this));

    this.subscribeMaybeDisableField("idpMetadataTLSVerifyPeer", "idpMetadataTLSCAs");
    this.subscribeMaybeDisableField("idpMetadataRefreshIntervalSFlag", "idpMetadataRefreshIntervalS");
    this.subscribeMaybeDisableField("usernameAttributeFlag", "usernameAttribute");
    this.subscribeMaybeDisableField("groupsAttributeFlag", "groupsAttribute");
    this.subscribeMaybeDisableField("groupsAttributeFlag", "groupsAttributeSep");
    this.subscribeMaybeDisableField("groupsAttributeFlag", "groupsFilterRE");
    this.subscribeMaybeDisableField("rolesAttributeFlag", "rolesAttribute");
    this.subscribeMaybeDisableField("rolesAttributeFlag", "rolesAttributeSep");
    this.subscribeMaybeDisableField("rolesAttributeFlag", "rolesFilterRE");

    this.form.group.get("spTrustedFingerprintsUsageEverything").valueChanges
      .pipe(distinctUntilChanged(),
            map((v) => !v),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, "spTrustedFingerprintsUsageMetadata"));

    // Warning streams.
    this.hasRemotePeerWarning =
      combineLatest(
          this.form.group.get('idpMetadataTLSVerifyPeer').valueChanges,
          this.form.group.get('spTrustedFingerprintsUsage').valueChanges)
      .pipe(map(this.hasRemotePeerWarning.bind(this)));

    this.hasSignatureWarning =
      combineLatest(
          this.form.group.get('spVerifyAssertionSig').valueChanges,
          this.form.group.get('spVerifyAssertionEnvelopSig').valueChanges)
      .pipe(map(this.hasSignatureWarning.bind(this)));
  }

  subscribeMaybeDisableField(flag, field) {
    combineLatest(
      this.form.group.get(flag).valueChanges,
      this.enabled
    )
      .pipe(
            takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, field));
  }

  packPostSaml() {
    const packedData = clone(this.form.group.value);

    // Read only values.
    delete packedData.spConsumeURL;
    delete packedData.spLogoutURL;
    delete packedData.spMetadataURL;
    delete packedData.idpMetadataTLSExtraOpts;

    if (packedData.usernameAttributeFlag === false) {
      packedData.usernameAttribute = '';
    }
    delete packedData.usernameAttributeFlag;

    if (packedData.groupsAttributeFlag === false) {
      packedData.groupsAttribute = '';
    }
    delete packedData.groupsAttributeFlag;

    if (packedData.rolesAttributeFlag === false) {
      packedData.rolesAttribute = '';
    }
    delete packedData.rolesAttributeFlag;

    if (packedData.idpMetadataOrigin === 'http') {
      delete packedData.idpMetadata;
    } else {
      delete packedData.idpMetadataURL;
    }

    if (!packedData.idpMetadataRefreshIntervalSFlag) {
      delete packedData.idpMetadataRefreshIntervalS;
    }
    if (packedData.idpMetadataOrigin === 'http' && !packedData.idpMetadataRefreshIntervalSFlag) {
      packedData.idpMetadataOrigin = 'http_one_time';
    }
    delete packedData.idpMetadataRefreshIntervalSFlag

    if (!packedData.spKey || packedData.spKey === '**********') {
      delete packedData.spKey;
    }

    if (!packedData.spCertificate) {
      delete packedData.spCertificate;
    }

    if (packedData.spVerifyRecipientFlag === false) {
      packedData.spVerifyRecipient = false;
    }
    delete packedData.spVerifyRecipientFlag;

    if (packedData.spAssertionDupeCheckFlag === false) {
      packedData.spAssertionDupeCheck = 'disabled';
    }
    delete packedData.spAssertionDupeCheckFlag;

    if (packedData.spTrustedFingerprints == "") {
      delete packedData.spTrustedFingerprints;
    }

    if (packedData.spTrustedFingerprints == "") {
      delete packedData.spTrustedFingerprints;
    }

    if (packedData.spTrustedFingerprintsUsageMetadata === true) {
      packedData.spTrustedFingerprintsUsage = 'metadataInitialOnly';
    } 

    if (packedData.spTrustedFingerprintsUsageEverything === true) {
      packedData.spTrustedFingerprintsUsage = 'everything';
    } 

    if (packedData.spTrustedFingerprintsUsageMetadataOnly === false &&
        packedData.spTrustedFingerprintsUsageEverything === false) {
      packedData.spTrustedFingerprintsUsage = 'metadataOnly';
    }

    if (packedData.spBaseURLType !== "custom") {
      delete packedData.spCustomBaseURL;
    }

    if (packedData.spVerifyRecipient !== "custom") {
      delete packedData.spVerifyRecipientValue;
    }

    delete packedData.spTrustedFingerprintsUsageEverything;
    delete packedData.spTrustedFingerprintsUsageMetadata;

    return packedData;
  }

  unpackGetSaml(data) {
    let unpackedData = clone(data);

    if (unpackedData.usernameAttribute) {
      unpackedData.usernameAttributeFlag = true;
    }

    if (unpackedData.groupsAttribute) {
      unpackedData.groupsAttributeFlag = true;
    }

    if (unpackedData.rolesAttribute) {
      unpackedData.rolesAttributeFlag = true;
    }

    if (unpackedData.idpMetadataOrigin === 'http') {
      unpackedData.idpMetadataRefreshIntervalSFlag = true;
    }
    if (unpackedData.idpMetadataOrigin === 'http_one_time') {
      unpackedData.idpMetadataOrigin = 'http';
      unpackedData.idpMetadataRefreshIntervalSFlag = false;
    }

    if (unpackedData.spVerifyRecipient) {
      unpackedData.spVerifyRecipientFlag = true;
    } else {
      unpackedData.spVerifyRecipientFlag = false;
      unpackedData.spVerifyRecipient = 'consumeURL'; //default when flag is enabled
    }

    if (unpackedData.spAssertionDupeCheck === 'disabled') {
      unpackedData.spAssertionDupeCheckFlag = false;
      unpackedData.spAssertionDupeCheck = 'global'; //default when flag is enabled
    } else {
      unpackedData.spAssertionDupeCheckFlag = true;
    }

    if (unpackedData.spTrustedFingerprintsUsage === 'metadataInitialOnly') {
      unpackedData.spTrustedFingerprintsUsageMetadata = true;
    }

    if (unpackedData.spTrustedFingerprintsUsage === 'everything') {
      unpackedData.spTrustedFingerprintsUsageEverything = true;
    }

    return unpackedData;
  }

  showServiceProviderMetaDataHasErrors([enabled, errors]) {
    const errorKeys = [
      'spEntityId',
      'spOrgName',
      'spContactName',
      'spOrgDisplayName',
      'spContactEmail',
      'spOrgURL',
      'spBaseURLType',
      'spBaseURLScheme',
      'spSignMetadata',
      'spCustomBaseURL',
    ];

    return enabled && this.hasKeyErrors(errorKeys, errors);
  }

  showKeyAndCertificatesHasErrors([enabled, errors]) {
    const errorKeys = ['spKey', 'spCertificate', 'spChain'];

    return enabled && this.hasKeyErrors(errorKeys, errors);
  }

  showIdentityProviderMetaDataHasErrors([enabled, errors]) {
    const errorKeys = [
      'idpMetadata',
      'idpMetadataURL',
      'idpMetadataOrigin',
      'idpMetadataRefreshIntervalS',
      'idpMetadataTLSVerifyPeer',
    ];

    return enabled && this.hasKeyErrors(errorKeys, errors);
  }

  showTrustedFingerprintsHasErrors([enabled, errors]) {
    const errorKeys = [
      'spTrustedFingerprints',
      'spTrustedFingerprintsUsage',
    ];

    return enabled && this.hasKeyErrors(errorKeys, errors);
  }

  showSingleSignOnHasErrors([enabled, errors]) {
    const errorKeys = ['authnNameIDFormat'];

    return enabled && this.hasKeyErrors(errorKeys, errors);
  }

  showAdvancedHasErrors([enabled, errors]) {
    const errorKeys = ['spMetadataCacheDuration', 'idpMetadataHttpTimeoutMs'];

    return enabled && this.hasKeyErrors(errorKeys, errors);
  }

  hasKeyErrors(errorKeys, errors) {
    if (errors && Array.isArray(errors.errors)) {
      return false;
    }

    return Object.keys(errors.errors).some(error => errorKeys.includes(error));
  }

  maybeDisableForm([enabled, writePermission]) {
    const method = (enabled && writePermission) ? "enable" : "disable";
    const settings = {emitEvent: false};

    this.form.group[method](settings);

    if (writePermission) {
      this.form.group.get('enabled').enable();
    }
  }

  maybeDisableField(field, [fieldEnable, formEnabled]) {
    this.form.group.get(field)[fieldEnable && formEnabled ? "enable": "disable"]({emitEvent: false});
  }

  hasRemotePeerWarning([verifyPeer, fingerprintUsage]) {
    return !verifyPeer && fingerprintUsage != 'everything';
  }

  hasSignatureWarning([assertSig, assertEnvSig]) {
    return !assertSig && !assertEnvSig;
  }

  cancel() {
    this.uiRouter.stateService.reload('app.admin.security.saml');
  }

  setIdpMetadata(value) {
    this.form.group.get('idpMetadata').setValue(value);
  }

  setSpKey(value) {
    this.form.group.get('spKey').setValue(value);
  }

  setSpCertificate(value) {
    this.form.group.get('spCertificate').setValue(value);
  }

  setSpChain(value) {
    this.form.group.get('spChain').setValue(value);
  }

  setIdpMetadataTLSCAs(value) {
    this.form.group.get('idpMetadataTLSCAs').setValue(value);
  }

  setSpTrustedFingerprints(value) {
    this.form.group.get('spTrustedFingerprints').setValue(value);
  }
}
