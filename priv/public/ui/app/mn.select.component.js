/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component, ViewChild} from '@angular/core';
import {BehaviorSubject, Subject} from 'rxjs';
import {FormBuilder} from '@angular/forms';
import {is} from 'ramda';
import {pluck, shareReplay, map, takeUntil, withLatestFrom,
        startWith, distinctUntilChanged} from 'rxjs/operators';
import {NgbDropdown} from '@ng-bootstrap/ng-bootstrap';

import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnHelperService} from './mn.helper.service.js';
import template from "./mn.select.html";

let isString = is(String);

export {MnSelectComponent};

class MnSelectComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-select",
      template,
      inputs: [
        "group",
        "mnFormControlName",
        "values",
        "labels",
        "valuesMapping",
        "capitalize",
        "mnPlaceholder",
        "placement",
        "hasSearch"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush,
      queries: {
        ngbDropdownView: new ViewChild(NgbDropdown)
      }
    })
  ]}

  static get parameters() { return [
    MnHelperService,
    FormBuilder
  ]}

  constructor(MnHelperService, FormBuilder) {
    super();

    this.mnHelperService = MnHelperService;
    this.hiddenRadioGroup = FormBuilder.group({
      hiddenRadio: null
    });

    this.id = this.mnHelperService.generateID();

    this.selectOptionClickStream = new Subject();
    this.selectLabelClickStream = new Subject();

    this.selectOptionClickStream
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.optionSelected.bind(this));
  }

  ngOnInit() {
    this.placement = this.placement || 'bottom';

    if (!this.valuesMapping) {
      this.valuesMapping = this.defaultValuesMapping.bind(this);
    }

    if (this.hasSearch) {
      this.prepareSearch();
    }

    this.dropdownFormControl = this.group.get(this.mnFormControlName);
    if (this.dropdownFormControl) {
      this.disabled = new BehaviorSubject(this.dropdownFormControl.disabled);
      this.dropdownFormControl.registerOnDisabledChange(disabled => this.disabled.next(disabled));

      this.value = this.dropdownFormControl.valueChanges.pipe(startWith(this.dropdownFormControl.value));
      this.value
        .pipe(distinctUntilChanged(),
              takeUntil(this.mnOnDestroy))
        .subscribe(this.setHiddenRadioValue.bind(this));
    }
  }

  prepareSearch() {
    let searchMinimumOptionsNumber = 10;
    this.searchFilter = this.mnHelperService.createFilter(this);

    var valuesStream = this.mnOnChanges
      .pipe(pluck("values", "currentValue"));

    this.preparedValues = valuesStream
      .pipe(this.searchFilter.pipe,
            shareReplay({refCount: true, bufferSize: 1}));

    this.hasSearchInput = valuesStream
      .pipe(map(values => (this.hasSearch && (values || []).length >= searchMinimumOptionsNumber) || false));

    var labelsStream = this.mnOnChanges
      .pipe(pluck("labels", "currentValue"));

    this.preparedLabels = labelsStream
      .pipe(this.searchFilter.pipe,
            shareReplay({refCount: true, bufferSize: 1}));

    this.selectLabelClickStream
      .pipe(withLatestFrom(labelsStream, valuesStream),
            takeUntil(this.mnOnDestroy))
      .subscribe(([selectedLabel, labels, values]) => {
        this.optionSelected(values[labels.indexOf(selectedLabel)]);
      });
  }

  setHiddenRadioValue(value) {
    let patchedValue = (this.hasSearch && this.labels) ?
      this.labels[this.values.indexOf(value)] : value;

    this.hiddenRadioGroup.patchValue({hiddenRadio: patchedValue});
  }

  /**
   * Default values mapping:
   * * if capitalize input flag is true - capitalize the displayed label if it is a string
   * * else leave the label as it is
   * @param option
   * @returns {string}
   */
  defaultValuesMapping(option) {
    if (this.capitalize && isString(option) && option) {
      return option[0].toUpperCase() + option.slice(1);
    }

    return option;
  }

  optionSelected(value) {
    this.dropdownFormControl.setValue(value);
    if (this.hasSearchInput) {
      this.searchFilter.group.get('value').setValue('');
    }

    this.ngbDropdownView.close();
  }
}
