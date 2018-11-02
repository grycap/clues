/*
#
# CLUES - Cluster Energy Saving System
# Copyright (C) 2018 - GRyCAP - Universitat Politecnica de Valencia
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

function StackedLineChart(chartid, data, yAxesID = [], onclick_legend = null, filter_legend = null) {
  var yAxes = yAxesID.map(function(id) {
    return {
      id: id,
      stacked: true,
      ticks: {
        min:0,
      },
      display: true
    }
  })
  return new Chart(
    $('#' + chartid),
    {
      type: "line",
      data: {
        datasets: data
      },
      options: {
        maintainAspectRatio: false,
        legend: {
          labels: {
            filter: filter_legend
          },
          onClick: onclick_legend
        },
        scales: {
          yAxes: yAxes,
          xAxes: [{
            type: "time",
            distribution: "linear",
            ticks: {
              source: "auto"
            },
            bounds: "data",
            gridLines: {
              display: false
            },
            time: {
              displayFormats: {
                minute: 'HH:mm',
                millisecond: 'HH:mm:ss.SSS',
                second: 'HH:mm:ss'
              }
            }                  
          }]        
        }
      }
    }
  )
}

function onClick_cohide(e, legendItem, _this, text = null) {
  var index = legendItem.datasetIndex;
  var double_click = isDblClick(_this, index);
  // This handler syncs the show of the number of slots used in one node with the slots available in that node
  var nodename = null;
  if (text!=null) {
    nodename = legendItem.text[0].slice(text.length);
  }
  var ci = _this.chart;
  var meta = ci.getDatasetMeta(index);
  meta.hidden = meta.hidden === null ? !meta.hidden : null;
  var hiddenval = meta.hidden

  if (double_click) {
    ci.data.datasets.forEach(function (e,i) {
      var meta = ci.getDatasetMeta(i);
      meta.hidden = true;
    })
    hiddenval = null;
    meta.hidden = null;
  }

  if (text != null) {
    ci.data.datasets.forEach(function(e,i) {
      if (i!==index) {
        var meta = ci.getDatasetMeta(i);
        if (e.label[0].slice(-nodename.length) == nodename) {
          meta.hidden = hiddenval;
        }
      }
    })
  }
  ci.update();
}

// Legend do not support double click, so we'll simulate it
function isDblClick(obj, idx) {
  var double_click = false;
  var t = Date.now();
  if (obj.click !== undefined) {
    double_click = ((idx == obj.click.idx) && (t-obj.click.t < 500));
  }
  obj.click = {
    t: t,
    idx: idx
  };
  return double_click;
}