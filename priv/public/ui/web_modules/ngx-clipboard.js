import './common/tslib.es6-c4a4947b.js';
import { a as Subject } from './common/mergeMap-64c6f393.js';
import './common/merge-183efbc7.js';
import './common/share-d41e3509.js';
import { InjectionToken, Injectable, Inject, Optional, defineInjectable, inject, Directive, Input, Output, HostListener, ViewContainerRef, TemplateRef, EventEmitter, NgModule } from './@angular/core.js';
import { DOCUMENT, CommonModule } from './@angular/common.js';

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
var WINDOW = new InjectionToken('WindowToken', typeof window !== 'undefined' && window.document ? { providedIn: 'root', factory: (/**
     * @return {?}
     */
    function () { return window; }) } : undefined);

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * The following code is heavily copied from https://github.com/zenorocha/clipboard.js
 */
var ClipboardService = /** @class */ (function () {
    function ClipboardService(document, window) {
        this.document = document;
        this.window = window;
        this.copySubject = new Subject();
        this.copyResponse$ = this.copySubject.asObservable();
        this.config = {};
    }
    /**
     * @param {?} config
     * @return {?}
     */
    ClipboardService.prototype.configure = /**
     * @param {?} config
     * @return {?}
     */
    function (config) {
        this.config = config;
    };
    /**
     * @param {?} content
     * @return {?}
     */
    ClipboardService.prototype.copy = /**
     * @param {?} content
     * @return {?}
     */
    function (content) {
        if (!this.isSupported || !content) {
            return this.pushCopyResponse({ isSuccess: false, content: content });
        }
        /** @type {?} */
        var copyResult = this.copyFromContent(content);
        if (copyResult) {
            return this.pushCopyResponse({ content: content, isSuccess: copyResult });
        }
        return this.pushCopyResponse({ isSuccess: false, content: content });
    };
    Object.defineProperty(ClipboardService.prototype, "isSupported", {
        get: /**
         * @return {?}
         */
        function () {
            return !!this.document.queryCommandSupported && !!this.document.queryCommandSupported('copy') && !!this.window;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @param {?} element
     * @return {?}
     */
    ClipboardService.prototype.isTargetValid = /**
     * @param {?} element
     * @return {?}
     */
    function (element) {
        if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
            if (element.hasAttribute('disabled')) {
                throw new Error('Invalid "target" attribute. Please use "readonly" instead of "disabled" attribute');
            }
            return true;
        }
        throw new Error('Target should be input or textarea');
    };
    /**
     * Attempts to copy from an input `targetElm`
     */
    /**
     * Attempts to copy from an input `targetElm`
     * @param {?} targetElm
     * @param {?=} isFocus
     * @return {?}
     */
    ClipboardService.prototype.copyFromInputElement = /**
     * Attempts to copy from an input `targetElm`
     * @param {?} targetElm
     * @param {?=} isFocus
     * @return {?}
     */
    function (targetElm, isFocus) {
        if (isFocus === void 0) { isFocus = true; }
        try {
            this.selectTarget(targetElm);
            /** @type {?} */
            var re = this.copyText();
            this.clearSelection(isFocus ? targetElm : undefined, this.window);
            return re && this.isCopySuccessInIE11();
        }
        catch (error) {
            return false;
        }
    };
    /**
     * This is a hack for IE11 to return `true` even if copy fails.
     */
    /**
     * This is a hack for IE11 to return `true` even if copy fails.
     * @return {?}
     */
    ClipboardService.prototype.isCopySuccessInIE11 = /**
     * This is a hack for IE11 to return `true` even if copy fails.
     * @return {?}
     */
    function () {
        /** @type {?} */
        var clipboardData = this.window['clipboardData'];
        if (clipboardData && clipboardData.getData) {
            if (!clipboardData.getData('Text')) {
                return false;
            }
        }
        return true;
    };
    /**
     * Creates a fake textarea element, sets its value from `text` property,
     * and makes a selection on it.
     */
    /**
     * Creates a fake textarea element, sets its value from `text` property,
     * and makes a selection on it.
     * @param {?} content
     * @param {?=} container
     * @return {?}
     */
    ClipboardService.prototype.copyFromContent = /**
     * Creates a fake textarea element, sets its value from `text` property,
     * and makes a selection on it.
     * @param {?} content
     * @param {?=} container
     * @return {?}
     */
    function (content, container) {
        if (container === void 0) { container = this.document.body; }
        // check if the temp textarea still belongs to the current container.
        // In case we have multiple places using ngx-clipboard, one is in a modal using container but the other one is not.
        if (this.tempTextArea && !container.contains(this.tempTextArea)) {
            this.destroy(this.tempTextArea.parentElement);
        }
        if (!this.tempTextArea) {
            this.tempTextArea = this.createTempTextArea(this.document, this.window);
            try {
                container.appendChild(this.tempTextArea);
            }
            catch (error) {
                throw new Error('Container should be a Dom element');
            }
        }
        this.tempTextArea.value = content;
        /** @type {?} */
        var toReturn = this.copyFromInputElement(this.tempTextArea, false);
        if (this.config.cleanUpAfterCopy) {
            this.destroy(this.tempTextArea.parentElement);
        }
        return toReturn;
    };
    /**
     * Remove temporary textarea if any exists.
     */
    /**
     * Remove temporary textarea if any exists.
     * @param {?=} container
     * @return {?}
     */
    ClipboardService.prototype.destroy = /**
     * Remove temporary textarea if any exists.
     * @param {?=} container
     * @return {?}
     */
    function (container) {
        if (container === void 0) { container = this.document.body; }
        if (this.tempTextArea) {
            container.removeChild(this.tempTextArea);
            // removeChild doesn't remove the reference from memory
            this.tempTextArea = undefined;
        }
    };
    /**
     * Select the target html input element.
     */
    /**
     * Select the target html input element.
     * @private
     * @param {?} inputElement
     * @return {?}
     */
    ClipboardService.prototype.selectTarget = /**
     * Select the target html input element.
     * @private
     * @param {?} inputElement
     * @return {?}
     */
    function (inputElement) {
        inputElement.select();
        inputElement.setSelectionRange(0, inputElement.value.length);
        return inputElement.value.length;
    };
    /**
     * @private
     * @return {?}
     */
    ClipboardService.prototype.copyText = /**
     * @private
     * @return {?}
     */
    function () {
        return this.document.execCommand('copy');
    };
    /**
     * Moves focus away from `target` and back to the trigger, removes current selection.
     */
    /**
     * Moves focus away from `target` and back to the trigger, removes current selection.
     * @private
     * @param {?} inputElement
     * @param {?} window
     * @return {?}
     */
    ClipboardService.prototype.clearSelection = /**
     * Moves focus away from `target` and back to the trigger, removes current selection.
     * @private
     * @param {?} inputElement
     * @param {?} window
     * @return {?}
     */
    function (inputElement, window) {
        inputElement && inputElement.focus();
        window.getSelection().removeAllRanges();
    };
    /**
     * Creates a fake textarea for copy command.
     */
    /**
     * Creates a fake textarea for copy command.
     * @private
     * @param {?} doc
     * @param {?} window
     * @return {?}
     */
    ClipboardService.prototype.createTempTextArea = /**
     * Creates a fake textarea for copy command.
     * @private
     * @param {?} doc
     * @param {?} window
     * @return {?}
     */
    function (doc, window) {
        /** @type {?} */
        var isRTL = doc.documentElement.getAttribute('dir') === 'rtl';
        /** @type {?} */
        var ta;
        ta = doc.createElement('textarea');
        // Prevent zooming on iOS
        ta.style.fontSize = '12pt';
        // Reset box model
        ta.style.border = '0';
        ta.style.padding = '0';
        ta.style.margin = '0';
        // Move element out of screen horizontally
        ta.style.position = 'absolute';
        ta.style[isRTL ? 'right' : 'left'] = '-9999px';
        // Move element to the same position vertically
        /** @type {?} */
        var yPosition = window.pageYOffset || doc.documentElement.scrollTop;
        ta.style.top = yPosition + 'px';
        ta.setAttribute('readonly', '');
        return ta;
    };
    /**
     * Pushes copy operation response to copySubject, to provide global access
     * to the response.
     */
    /**
     * Pushes copy operation response to copySubject, to provide global access
     * to the response.
     * @param {?} response
     * @return {?}
     */
    ClipboardService.prototype.pushCopyResponse = /**
     * Pushes copy operation response to copySubject, to provide global access
     * to the response.
     * @param {?} response
     * @return {?}
     */
    function (response) {
        this.copySubject.next(response);
    };
    /**
     * @deprecated use pushCopyResponse instead.
     */
    /**
     * @deprecated use pushCopyResponse instead.
     * @param {?} response
     * @return {?}
     */
    ClipboardService.prototype.pushCopyReponse = /**
     * @deprecated use pushCopyResponse instead.
     * @param {?} response
     * @return {?}
     */
    function (response) {
        this.pushCopyResponse(response);
    };
    ClipboardService.decorators = [
        { type: Injectable, args: [{ providedIn: 'root' },] }
    ];
    /** @nocollapse */
    ClipboardService.ctorParameters = function () { return [
        { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] },
        { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [WINDOW,] }] }
    ]; };
    /** @nocollapse */ ClipboardService.ngInjectableDef = defineInjectable({ factory: function ClipboardService_Factory() { return new ClipboardService(inject(DOCUMENT), inject(WINDOW, 8)); }, token: ClipboardService, providedIn: "root" });
    return ClipboardService;
}());

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
var ClipboardDirective = /** @class */ (function () {
    function ClipboardDirective(clipboardSrv) {
        this.clipboardSrv = clipboardSrv;
        this.cbOnSuccess = new EventEmitter();
        this.cbOnError = new EventEmitter();
    }
    // tslint:disable-next-line:no-empty
    // tslint:disable-next-line:no-empty
    /**
     * @return {?}
     */
    ClipboardDirective.prototype.ngOnInit = 
    // tslint:disable-next-line:no-empty
    /**
     * @return {?}
     */
    function () { };
    /**
     * @return {?}
     */
    ClipboardDirective.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.clipboardSrv.destroy(this.container);
    };
    /**
     * @param {?} event
     * @return {?}
     */
    ClipboardDirective.prototype.onClick = /**
     * @param {?} event
     * @return {?}
     */
    function (event) {
        if (!this.clipboardSrv.isSupported) {
            this.handleResult(false, undefined, event);
        }
        else if (this.targetElm && this.clipboardSrv.isTargetValid(this.targetElm)) {
            this.handleResult(this.clipboardSrv.copyFromInputElement(this.targetElm), this.targetElm.value, event);
        }
        else if (this.cbContent) {
            this.handleResult(this.clipboardSrv.copyFromContent(this.cbContent, this.container), this.cbContent, event);
        }
    };
    /**
     * Fires an event based on the copy operation result.
     * @param succeeded
     */
    /**
     * Fires an event based on the copy operation result.
     * @private
     * @param {?} succeeded
     * @param {?} copiedContent
     * @param {?} event
     * @return {?}
     */
    ClipboardDirective.prototype.handleResult = /**
     * Fires an event based on the copy operation result.
     * @private
     * @param {?} succeeded
     * @param {?} copiedContent
     * @param {?} event
     * @return {?}
     */
    function (succeeded, copiedContent, event) {
        /** @type {?} */
        var response = {
            isSuccess: succeeded,
            event: event
        };
        if (succeeded) {
            response = Object.assign(response, {
                content: copiedContent,
                successMessage: this.cbSuccessMsg
            });
            this.cbOnSuccess.emit(response);
        }
        else {
            this.cbOnError.emit(response);
        }
        this.clipboardSrv.pushCopyResponse(response);
    };
    ClipboardDirective.decorators = [
        { type: Directive, args: [{
                    selector: '[ngxClipboard]'
                },] }
    ];
    /** @nocollapse */
    ClipboardDirective.ctorParameters = function () { return [
        { type: ClipboardService }
    ]; };
    ClipboardDirective.propDecorators = {
        targetElm: [{ type: Input, args: ['ngxClipboard',] }],
        container: [{ type: Input }],
        cbContent: [{ type: Input }],
        cbSuccessMsg: [{ type: Input }],
        cbOnSuccess: [{ type: Output }],
        cbOnError: [{ type: Output }],
        onClick: [{ type: HostListener, args: ['click', ['$event.target'],] }]
    };
    return ClipboardDirective;
}());

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
var ClipboardIfSupportedDirective = /** @class */ (function () {
    function ClipboardIfSupportedDirective(_clipboardService, _viewContainerRef, _templateRef) {
        this._clipboardService = _clipboardService;
        this._viewContainerRef = _viewContainerRef;
        this._templateRef = _templateRef;
    }
    /**
     * @return {?}
     */
    ClipboardIfSupportedDirective.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        if (this._clipboardService.isSupported) {
            this._viewContainerRef.createEmbeddedView(this._templateRef);
        }
    };
    ClipboardIfSupportedDirective.decorators = [
        { type: Directive, args: [{
                    selector: '[ngxClipboardIfSupported]'
                },] }
    ];
    /** @nocollapse */
    ClipboardIfSupportedDirective.ctorParameters = function () { return [
        { type: ClipboardService },
        { type: ViewContainerRef },
        { type: TemplateRef }
    ]; };
    return ClipboardIfSupportedDirective;
}());

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
var ClipboardModule = /** @class */ (function () {
    function ClipboardModule() {
    }
    ClipboardModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule],
                    declarations: [ClipboardDirective, ClipboardIfSupportedDirective],
                    exports: [ClipboardDirective, ClipboardIfSupportedDirective]
                },] }
    ];
    return ClipboardModule;
}());

export { ClipboardDirective, ClipboardIfSupportedDirective, ClipboardModule, ClipboardService };
