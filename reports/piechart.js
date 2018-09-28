Vue.component('piechart', {
  props: [ 'percentage', 'title', 'duration'],
  computed: {
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
          return moment.duration(duration).humanize();
        return "";
      }
  },
  template: `        
  <svg width="100%" height="100%" viewBox="0 0 42 42" class="donut">
    <circle class="donut-hole" cx="21" cy="21" r="15.91549430918954" fill="#fff"></circle>
    <circle class="donut-ring" cx="21" cy="21" r="15.91549430918954" fill="transparent" stroke="#d2d3d4" stroke-width="5"></circle>
    <circle class="donut-segment" cx="21" cy="21" r="15.91549430918954" fill="transparent" :stroke="color" stroke-width="5" :stroke-dasharray="percentage + ' ' + (100 - percentage)" stroke-dashoffset="25"></circle>
    <g class="chart-text">
        <text x="50%" y="50%" class="chart-number">
          {{percentage.toFixed(1)}}%
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
