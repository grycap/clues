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

Vue.component('minibarchart', {
  props: [ 'values', 'labels' ],
    computed: {
      pvalues: function() {
        var p = [];
        for (var i in this.values) {
          p.push({
            value: parseFloat(this.values[i]),
            label: (this.labels !== undefined)?this.labels[i]:null
          })
        }
        return p;
      }
    },
    render: function(createElement) {
        var bars = [];
        var sum = 0;
        this.pvalues.forEach((x) => sum+=x.value )
        bars = this.pvalues.map(function(x) {
            var v = Math.round(100.0*x.value/sum);
            var progressbar = createElement('div', {
              class: {
                  'progress-bar':true
              },
              attrs: {
                  'role':'progressbar',
                  'aria-valuenow': v,
                  'aria-valuemin': 0,
                  'aria-valuemax': 100
              },
              style: 
                  'width: '+v+'%;'
              
          }, [x.value]);

          var label = createElement('span', {
            class: {
                'pull-left':true,
                'progress-label':true
            },
          }, [x.label]);


            return createElement('div', {
            class: 'progress'
        }, [label, progressbar])
        })
        return createElement('div', {'class':'container'}, [bars]);
    }
})