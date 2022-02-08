/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import _ from "lodash";
import {select} from "d3-selection";
import {bisector} from "d3-array";
import {schemeTableau10} from "d3-scale-chromatic";
import {scaleTime, scaleLinear} from "d3-scale";
import {axisBottom, axisLeft, axisRight} from "d3-axis";
import {line as d3Line} from "d3-shape";
import {interpolateTransformSvg} from "d3-interpolate";
import {brushX, brushSelection} from "d3-brush";
import {max as d3Max} from "d3-array"

export default "mnD3Service";

angular
  .module('mnD3Service', [])
  .factory('mnD3Service', mnD3ServiceFactory);

function mnD3ServiceFactory() {
  class mnD3 {
    constructor(options, rootElement) {
      this.opt = options;
      this.cht = this.opt.chart;
      this.rootEl = select(rootElement);
      this.throttledResize = _.throttle(this.resize.bind(this), 30);
      var elmRect = this.getElementRect();
      this.cvsRect = this.getCanvasRect(elmRect);
      this.colors = this.cht.color || schemeTableau10;

      //main container

      this.id = Math.random().toString(36).substr(2, 9);

      let root = this.rootEl
          .append("svg")
          .attr("width", "100%")
          .attr("height", this.cht.height);

      this.svg = root
        .append("g")
        .attr("transform", this.getTransform(this.cht.margin.left, this.cht.margin.top));

      let defs =
          root.append("defs")

      this.clip = defs.append("rect")
        .attr("id", "clipRect" + this.id)
        .attr("width", this.cvsRect.width)
        .attr("height", this.cvsRect.height)
        .attr("x", 0)
        .attr("y", 0);

      defs.append("clipPath")
        .attr("id", "clipPath" + this.id)
        .append("use")
        .attr("xlink:href", "#clipRect" + this.id);
    }
    init() {
      this.inititalized = true;
      this.svg.html("");

      this.linesWrap = this.svg.append("g")
        .attr("clip-path", "url(#clipPath" +this.id+ ")")
        .attr("fill", "rgba(0, 0, 0, 0.002)");

      this.linesWrap.append( "use" ).attr( "xlink:href", "#clipRect" + this.id);

      // Initialise a X axis:
      this.xScale = scaleTime().range([0, this.cvsRect.width]);
      this.xAxis = axisBottom().scale(this.xScale).tickFormat(this.cht.xAxis.tickFormat);
      this.svg.append("g").attr("transform", this.getTransform(0, this.cvsRect.height))
        .attr("class", "xAxis");

      // Initialise a Y axis and lines
      this.yScale = [];
      this.yAxis = [];
      this.yLines = [];
      this.createYAxis(0, axisLeft, this.getTransform(0, 0));
      if (this.cht.yAxis[1]) {
        this.createYAxis(1, axisRight, this.getTransform(this.cvsRect.width, 0));
      }
    }
    destroy() {
      this.svg.remove();
    }
    getCanvasRect(elmRect) {
      return {
        width: elmRect.width - this.cht.margin.left - this.cht.margin.right,
        height: this.cht.height - this.cht.margin.bottom - this.cht.margin.top
      };
    }
    updateYAxis(i) {
      var domain = this.data.filter(function (line) {
        return (line.yAxis == i);
      }.bind(this));

      var yDomain = this.cht.yAxis[i].domain(domain);

      this.yScale[i].domain(yDomain);
      if (!this.cht.showTicks && domain.length) {
        this.yAxis[i].tickValues(yDomain);//show min/max only
      }

      this.svg.selectAll(".yAxis" + i).call(this.yAxis[i]).transition().duration(0);
    }
    createYAxis(i, axis, coordinates) {
      this.yScale[i] = scaleLinear().range([this.cvsRect.height, 0]).nice();
      this.yAxis[i] = axis().scale(this.yScale[i]).tickFormat(this.cht.yAxis[i].tickFormat);
      this.yLines[i] = d3Line()
        .defined((d, i, lines) => {
          let isValueAvailable = !isNaN(d[1]);
          if (this.opt.is70Cluster && isValueAvailable && i) {
            isValueAvailable = (d[0] - lines[i-1][0]) == this.opt.step;
          }
          return isValueAvailable;
        })
        .x(d => this.xScale(d[0]))
        .y(d => this.yScale[i](d[1]));

      if (!this.cht.hideTicks) {
        this.svg.append("g").attr("transform", coordinates).attr("class", "yAxis yAxis" + i);
      }
    }
    getElementRect() {
      return this.rootEl.node().getBoundingClientRect();
    }
    getTransform(x, y) {
      return "translate(" + x + ", " + y + ")";
    }
    redrawXAxis() {
      return this.svg.selectAll(".xAxis").transition().duration(0).call(this.xAxis);
    }
    drawLine(path) {
      path.attr("d", function (d) {
        if (!d.disabled) {
          return this.yLines[d.yAxis](d.values);
        } else {
          return undefined;
        }
      }.bind(this))
        .call(this.drawLinePath.bind(this));
    }
    drawLinePath(pipe) {
      pipe.style("stroke", v => v.color)
        .style("fill", "none")
        .style("stroke-width", 1);
    }
    drawCirclePath(pipe) {
      pipe.style("fill", v => v.color);
    }
    updateXAxis(xDomain) {
      if (!xDomain.length) {
        return;
      }
      this.xScale.domain(xDomain);
      this.redrawXAxis();
    }
    cancelLineAnimation() {
      this.animationInterval && window.clearInterval(this.animationInterval);
    }
    stopLineAnimation() {
      this.cancelLineAnimation();
      this.linesWrap.selectAll('.lines').attr("transform", null);
    }
    animateLine(fps) {
      let frame = 0;
      let v = this.xAxisTimestamps;
      let interpolateSvg = interpolateTransformSvg(
        this.getTransform(0, 0),
        this.getTransform(this.xScale(v[1]), 0));

      let layer = this.linesWrap.selectAll('.lines');

      this.animationInterval = window.setInterval(() => {
        frame++;

        let transformValue = interpolateSvg(-(frame / fps));

        layer.attr("transform", transformValue);

        let step = (v[1] - v[0]) * (frame / fps);

        let domain = this.getXaxisDomain(this.xAxisTimestamps);

        this.updateXAxis([domain[0] + step, domain[1] + step]);

        if (frame >= fps) {
          window.clearInterval(this.animationInterval);
        }
      }, (v[1] - v[0]) / fps);
    }
    updateLine() {
      this.opt.enableAnimation && this.stopLineAnimation();

      this.linesWrap.selectAll('.lines')
        .data(this.data)
        .join(function (enter) {
          return enter
            .append('path').attr('class', 'lines')
            .call(this.drawLine.bind(this));
        }.bind(this), function (update) {
          return update.call(this.drawLine.bind(this));
        }.bind(this));

      this.opt.enableAnimation && this.animateLine(100);
    }
    getxAxisTimestamps(data) {
      let rv = [];
      let step = this.opt.step;
      let lengthInMS = this.opt.start;
      let start = d3Max(data, v => v.startTimestamp);

      while (lengthInMS > 0) {
        rv.push(start);
        lengthInMS -= step;
        start += step;
      }

      return rv;
    }
    updateData(data) {
      if (!data) {
        return;
      }

      data.forEach((item, index) => {
        item.color = this.colors[index % this.colors.length];
      });

      if (this.opt.is70Cluster) {

        this.xAxisTimestamps = this.getxAxisTimestamps(data);
        this.xAxisMap = data.map(line => line.values.reduce((acc, [ts, v]) => {
          acc[ts] = v;
          return acc;
        }, {}));

        this.data = data;

        if (!this.inititalized) {
          this.init();
        }
        this.updateXAxis(this.getXaxisDomain(this.xAxisTimestamps));
      } else {
        this.xAxisData = data
          .reduce((max, item) =>
                  item.values.length > max.length ? item.values : max
                  , []);

        this.data = data;

        if (this.xAxisData && this.xAxisData.length) {
          if (!this.inititalized) {
            this.init();
          }
        } else {
          this.showEmptyContent();
          return;
        }
        this.updateXAxis(this.getXaxisDomain(this.xAxisData));
      }

      this.updateYAxis(0);

      if (this.cht.yAxis[1]) {
        this.updateYAxis(1);
      }

      this.updateLine();

      return true;
    }
    showEmptyContent() {
      this.inititalized = false;
      this.svg.html("<text class='charts-nodata'>"+this.cht.noData+"</text>");
    }
    getXaxisDomain(data) {
      if (this.opt.is70Cluster) {
        if (this.opt.enableAnimation) {
          return [data[0], data[data.length-3]];
        } else {
          return [data[0], data[data.length-1]];
        }
      } else {
        return [data[0][0], data[data.length-1][0]];
      }
    }
    toggleLine(i) {
      var maybeLast = this.data.filter(v => !v.disabled);
      if ((maybeLast.length == 1) && (this.data.indexOf(maybeLast[0]) == i)) {
        return;
      }
      this.data[i].disabled = !this.data[i].disabled;
      this.updateData(this.data);
      return true;
    }
    resize() {
      this.cvsRect = this.getCanvasRect(this.getElementRect());
      this.clip.attr("width", this.cvsRect.width).attr("height", this.cvsRect.height);
      this.xScale.range([0, this.cvsRect.width]);
      this.xAxis.ticks(Math.max(this.cvsRect.width/100, 2));
      if (this.cht.yAxis[1] && !this.cht.hideTicks) {
        this.svg.select(".yAxis1").attr("transform", this.getTransform(this.cvsRect.width, 0));
      }
      this.updateData(this.data);
    }
  }

  class mnD3Focus extends mnD3 {
    constructor(options, rootElement, chart) {
      super(options, rootElement[0]);
      this.chart = chart;
    }
    init() {
      super.init();

      if (this.opt.is70Cluster) {
        this.bisect = bisector(d => d).left;
      } else {
        this.bisect = bisector(function (d) { return d[0]; }).left;
      }

      this.brush = brushX()
        .extent([[0, 0], [this.cvsRect.width,
                          this.cht.height]])
        .on("brush end", this.brushed.bind(this));

      this.brushEl = this.svg.append("g")
        .attr("class", "charts-brush");

      this.svg.attr("class", "focus-chart");

      this.brushEl
        .call(this.brush)
        .call(this.brush.move, null);

      this.onInit && this.onInit();
    }
    getDomain() {
      var s = brushSelection(this.brushEl.node());
      let data = this.opt.is70Cluster ? this.xAxisTimestamps : this.xAxisData;
      return s ? s.map(this.xScale.invert, this.xScale) : this.getXaxisDomain(data);
    }
    brushed() {
      if (!this.data) {
        return;
      }
      var domain = this.getDomain();
      this.chart.updateXAxis(domain);
      this.chart.updateLine();
      this.chart.drawTooltip();
    }
    updateData(data) {
      if (!super.updateData(data)) {
        return;
      }
      this.brushed();
    }
    resize() {
      var s = this.getDomain();

      super.resize();

      this.brush.extent([[0, 0], [this.cvsRect.width, this.cht.height]]);
      this.brushEl.call(this.brush);

      if (brushSelection(this.brushEl.node())) { //proportional resize of selection
        let data = this.opt.is70Cluster ? this.xAxisTimestamps : this.xAxisData;
        var i1 = this.bisect(data, s[0]);
        var i2 = this.bisect(data, s[1]);

        this.brush.move(this.brushEl, [
          this.xScale(this.opt.is70Cluster ? data[i1] : data[i1][0]),
          this.xScale(this.opt.is70Cluster ? data[i2] : data[i2][0])
        ]);

        this.brushed();
      }
    }
  }

  class mnD3Tooltip extends mnD3 {
    constructor(options, rootElement, onInit) {
      super(options, rootElement[0]);
      this.onInit = onInit;
    }
    init() {
      super.init();

      if (this.opt.is70Cluster) {
        this.bisect = bisector(d => d).left;
      } else {
        this.bisect = bisector(function (d) { return d[0]; }).left;
      }

      //Tooltip
      this.tip = select("body").append("div").attr('class', 'mnd3-tooltip');
      this.tipLineWrap = this.svg.append("g").attr("class", "tip-line-wrap");
      this.tipLineWrap.append("path").attr("class", "tip-line").style("opacity", 0);
      this.tipBox = this.svg.append('rect')
        .attr("height", this.cvsRect.height)
        .attr("width", this.cvsRect.width).attr('opacity', 0);

      this.drawTooltipThrottle = _.throttle(this.drawTooltip.bind(this), 10, {leading: true});

      angular.element(this.tipBox.node()).on('mousemove', this.setMouseMoveEvent.bind(this));
      angular.element(this.tipBox.node()).on('mousemove', this.drawTooltipThrottle);
      angular.element(this.tipBox.node()).on('mouseout', this.hideTooltip.bind(this));
      if (this.opt.isPauseEnabled) {
        angular.element(this.tipBox.node()).on('mousemove', this.cancelLineAnimation.bind(this));
      }

      this.onInit && this.onInit();
    }
    destroy() {
      this.tip && this.tip.remove();
      this.svg && this.svg.remove();
    }
    showEmptyContent() {
      super.showEmptyContent();
      this.tip && this.tip.remove();
      this.legendsWrap && this.legendsWrap.remove();
    }
    updateData(data) {
      if (!super.updateData(data)) {
        return;
      }
      this.drawTooltip();
    }

    toggleLine(i) {
      if (!super.toggleLine(i)) {
        return;
      }
      select(this.getLegends.bind(this)().nodes()[i]).classed('disabled', this.data[i].disabled);
    }
    resize() {
      super.resize();
      if (this.tipBox) {
        this.tipBox.attr("height", this.cvsRect.height).attr("width", this.cvsRect.width);
        this.tipBoxRect = this.tipBox.node().getBoundingClientRect();
      }
    }
  }

  mnD3Tooltip.prototype.updateLabelRow = updateLabelRow;
  mnD3Tooltip.prototype.updateCirclePosition = updateCirclePosition;
  mnD3Tooltip.prototype.drawTooltip = drawTooltip;
  mnD3Tooltip.prototype.drawCircle = drawCircle;
  mnD3Tooltip.prototype.setMouseMoveEvent = setMouseMoveEvent;
  mnD3Tooltip.prototype.hideTooltip = hideTooltip;
  mnD3Tooltip.prototype.disableTooltip = disableTooltip;
  mnD3Tooltip.prototype.drawLegends = drawLegends;
  mnD3Tooltip.prototype.getLegends = getLegends;
  mnD3Tooltip.prototype.getLabelRowValues = getLabelRowValues;

  function getLegends() {
    return this.legendsWrap.selectAll('.legends');
  }

  function drawCircle(path) {
    path
      .call(this.drawCirclePath.bind(this))
      .call(this.updateCirclePosition.bind(this),
            this.opt.is70Cluster ? undefined : this.selectedValueIndex);
  }

  function drawTooltip() {
    if (!this.mouseMoveEvent || !this.data) {
      return;
    }

    var elementRect = this.tipBoxRect;

    var elementX = this.mouseMoveEvent.pageX - elementRect.left;

    var circlesPerLine;
    if (this.opt.is70Cluster) {

      let xDate = this.xScale.invert(elementX).getTime();
      let i = this.bisect(this.xAxisTimestamps, xDate);
      let d0 = this.xAxisTimestamps[i - 1];
      let d1 = this.xAxisTimestamps[i];
      this.selectedXDate = (!d0 || ((xDate - d0) > (d1 - xDate))) ? d1 : d0;

      this.svg.select(".tip-line")
        .style("opacity", "1")
        .attr("d", () => {
          var x = this.xScale(this.selectedXDate);
          x = x < 0 ? 0 : x > this.cvsRect.width ? this.cvsRect.width : x;
          var d = "M" + x  + "," + elementRect.height;
          d += " " + x + ", 0";
          return d;
        });

      circlesPerLine =
        this.tipLineWrap.selectAll('.circle-per-line')
        .data(this.data)
        .style('opacity', function (d, i) {
          var value = this.xAxisMap[i][this.selectedXDate];
          return ((value || value === 0) && !d.disabled) ? 1 : 0;
        }.bind(this));
    } else {

      let xDate = this.xScale.invert(elementX);
      let i = this.bisect(this.xAxisData, xDate);
      let d0 = this.xAxisData[i - 1];
      let d1 = this.xAxisData[i];

      // work out which date value is closest to the mouse
      this.selectedValueIndex = (!d0 || (xDate - d0[0]) > (d1[0] - xDate)) ? i : i-1;

      this.svg.select(".tip-line")
        .style("opacity", "1")
        .attr("d", function () {
          var idx = this.selectedValueIndex;
          var d = "M" + this.xScale(this.xAxisData[idx][0]) + "," + elementRect.height;
          d += " " + this.xScale(this.xAxisData[idx][0]) + ", 0";
          return d;
        }.bind(this));

      circlesPerLine =
        this.tipLineWrap.selectAll('.circle-per-line')
        .data(this.data)
        .style('opacity', function (d) {
          var idx = this.selectedValueIndex;
          return (d.values.length && !d.disabled && !isNaN(d.values[idx] && d.values[idx][1])) ? 1 : 0;
        }.bind(this));
    }

    circlesPerLine.join(function (enter) {
      enter
        .append("circle")
        .attr('class', 'circle-per-line')
        .attr("r", 5)
        .call(this.drawCircle.bind(this));
    }.bind(this), function (update) {
      update
        .transition()
        .duration(0)
        .call(this.drawCircle.bind(this));
    }.bind(this));

    let offsetWidth = document.getElementsByTagName("body")[0].clientWidth;
    let offsetHeight = document.getElementsByTagName("body")[0].clientHeight;
    let rightPos = offsetWidth - this.mouseMoveEvent.pageX;
    let bottomPos = offsetHeight - this.mouseMoveEvent.pageY;
    let leftRight = rightPos < 200 ? "right" : "left";
    let topBottom = bottomPos < 150 ? "bottom" : "top";

    if (!this.disableTooltipFlag) {
      var tooltipRows = this.tip
          .style('display', 'block')
          .style(leftRight == "left" ? "right" : "left", "auto")
          .style(topBottom == "top" ? "bottom" : "top", "auto")
          .style(leftRight, (leftRight == "left" ? this.mouseMoveEvent.pageX + 30 : rightPos) + "px")
          .style(topBottom, (topBottom == "top" ? this.mouseMoveEvent.pageY + 20 : bottomPos) + "px")
          .selectAll(".charts-tooltip-row")
          .data(this.data
                .map(this.getLabelRowValues.bind(this)),
                line => line.color+line.key+line.value+line.yAxis+line.disabled);

      tooltipRows.join((enter) => {
        enter
          .append("div")
          .attr('class', line => line.disabled ? "" : "charts-tooltip-row")
          .html(line =>  line.disabled ? "" : this.updateLabelRow(line))
      });
    }
  }

  function getLabelRowValues(line, i) {
    let value;
    if (this.opt.is70Cluster) {
      let selectedValue = this.xAxisMap[i][this.selectedXDate];
      let isValuePresent = (selectedValue || selectedValue === 0);
      value = isValuePresent ?
        this.cht.tooltip.valueFormatter(selectedValue, line.unit) : "-";
    } else {
      let idx = this.selectedValueIndex;
      value = (!line.values[idx] || line.values[idx][1] == undefined) ? "-" :
        this.cht.tooltip.valueFormatter(line.values[idx][1], line.unit);
    }
    return {
      disabled: line.disabled,
      yAxis: line.yAxis,
      color: line.color,
      key: line.key,
      value: value
    };
  }

  function drawLegends() {
    //Legends
    this.legendsWrap =
      this.rootEl.append("div").attr("class", "legends-wrap")
      .append("div").attr("class", "charts-filter-icon");

    this.getLegends()
      .data(this.data)
      .join(function (enter) {
        enter
          .append("div")
          .attr('class', 'legends')
          .html(getLegendsHtml.bind(this));
      }.bind(this), function (update) {
        update
          .html(getLegendsHtml.bind(this));
      }.bind(this));

    this.clickCB = this.getLegends().nodes().map(function (node, i) {
      var cb = function () {
        this.toggleLine(i);
        this.rootEl.dispatch('toggleLegend', {detail: {index: i}});
      }.bind(this);
      angular.element(node).on('click', cb);
      return cb;
    }.bind(this));
  }

  function getLegendsHtml(line) {
    return "<i style='background-color:" + line.color + "'></i>" +
      "<span>" + line.key + "</span>";
  }

  function updateLabelRow(line) {
    return "<span><i style='background-color:" + line.color + "'></i>" +
      "<span class='charts-tooltip-key'>" + line.key + (line.yAxis ? " (2nd axis)" : "") + "</span></span>" +
      "<span class='bold'>" + line.value + "</span>";
  }

  function updateCirclePosition(pipe, idx) {
    return this.opt.is70Cluster ? pipe.attr("transform", (line, i) => {
      let selectedValue = this.xAxisMap[i][this.selectedXDate];
      if (selectedValue || selectedValue === 0) {
        let x = this.xScale(this.selectedXDate);
        x = x < 0 ? 0 : x > this.cvsRect.width ? this.cvsRect.width : x;
        return this.getTransform(x, this.yScale[line.yAxis](selectedValue));
      }
    }) : pipe.attr("transform", function (line) {
      if (line.values[idx] && line.values[idx].length && !isNaN(line.values[idx][1])) {
        return this.getTransform(this.xScale(line.values[idx][0]),
                                 this.yScale[line.yAxis](line.values[idx][1]));
      }
    }.bind(this))
  }

  function hideTooltip() {
    this.mouseMoveEvent = false;
    this.svg.selectAll(".tip-line").style("opacity", 0);
    this.svg.selectAll(".circle-per-line").style("opacity", 0);
    this.tip.style("display", "none");
  }

  function disableTooltip(flag) {
    this.disableTooltipFlag = flag;
  }

  function setMouseMoveEvent(e) {
    this.mouseMoveEvent = e;
  }

  return {
    mdD3: mnD3,
    mnD3Focus: mnD3Focus,
    mnD3Tooltip: mnD3Tooltip,
  };
}
