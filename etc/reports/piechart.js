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

Vue.component('piechart', {
  props: [ 'percentage', 'title', 'duration'],
  computed: {
      _percentage: function() {
        return this.percentage == undefined || isNaN(this.percentage) ?0:this.percentage;
      },
      color: function() {
        switch (this.title) {
          case "error(*)": return "#ff6f69";
          case "off": return "#88d8b0"
          case "idle": return "#ffcc5c";
          case "used": return "#696fff";
          case "unknown": return "#626364";
          case "power cycle": return "#cc5cff";
          default: return "#96ceb4";
        }
      },
      title2: function() {
        var duration = this.duration;
        if (duration > 0)
          return moment.duration(duration, "seconds").humanize();
        return "";
      }
  },
  template: `        
  <svg width="100%" height="100%" viewBox="0 0 42 42" class="donut">
    <circle class="donut-hole" cx="21" cy="21" r="15.91549430918954" fill="#fff"></circle>
    <circle class="donut-ring" cx="21" cy="21" r="15.91549430918954" fill="transparent" stroke="#d2d3d4" stroke-width="5"></circle>
    <circle class="donut-segment" cx="21" cy="21" r="15.91549430918954" fill="transparent" :stroke="color" stroke-width="5" :stroke-dasharray="_percentage + ' ' + (100 - _percentage)" stroke-dashoffset="25"></circle>
    <g class="chart-text">
        <text x="50%" y="50%" class="chart-number">
          {{_percentage.toFixed(1)}}%
        </text>
        <text x="50%" y="50%" class="chart-label">
          <tspan x="50%" y="50%">
          {{title}}
          </tspan>
          <tspan  x="50%" y="50%" dy="1.2em">
          {{title2}}
        </tspan>
        </text>
    </g>                      
  </svg>                
  `
})
