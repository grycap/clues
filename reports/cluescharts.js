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
                minute: 'X',
                millisecond: 'X',
                second: 'X'
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