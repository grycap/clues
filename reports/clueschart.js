(function( doc, win ) {

    var clueschart,
        namespace = "http://www.w3.org/2000/svg";

    function isDefined( input ) {
        return typeof input !== "undefined";
    }

    function float( input ) {
        return parseFloat( input, 10 );
    }

    function truth( input ) {
        return isDefined( input ) && ( input === true || input === "true" );
    }

    function setNodeAttributes(node, v) {
        for (var p in v)
          node.setAttributeNS(null, p.replace(/[A-Z]/g, function(m, p, o, s) { return "-" + m.toLowerCase(); }), v[p]);
        return node
    }
    function createSVGNode(name, v) {
        var node = document.createElementNS("http://www.w3.org/2000/svg", name);
        return setNodeAttributes(node, v);
    }  

    clueschart = win.Clueschart = function( el, options ) {
        console.log(el);
        if ( el && typeof el === "string" ) {
            this.$wrapper = doc.querySelectorAll( el )[0];
        } else if ( el instanceof window.HTMLElement ) {
            this.$wrapper = el;
        } else {
            this.$wrapper = doc.body;
            options = el;
        }

        this.state =               {};
        this.options =             options || {};
        this.options.value =       isDefined( this.options.value ) ? float( this.options.value ) : 50;
        this.options.color =       this.options.color || "mediumslateblue";
        this.options.transition =  this.options.transition || "all 1.2s cubic-bezier(0.57, 0.13, 0.18, 0.98)";
        this.options.text =        isDefined( this.options.text) ? this.options.text : false;

        console.log(this);
        this.createChart();
        return this;
    };


    clueschart.prototype.createChart = function() {
        // Based on this article, recalculating to 100px
        // https://medium.com/@heyoka/scratch-made-svg-donut-pie-charts-in-html5-2c587e935d72

        var w = $(this.$wrapper).width();
        var h = $(this.$wrapper).height();
        w = float(w<h?w:h)/100.0;

        const TEMPLATE = {
            cx: 50 * w,
            cy: 50 * w,
            r: 42 * w,
            strokeWidth: 10 * w,
            circ: 2.638937829 * w // 2 * pi * r / 100 (because of the percentage)
        }

        this.$html = doc.createDocumentFragment();
        this.$svg = createSVGNode('svg', {viewbox: '0 0 ' + 100 * w + ' ' + 100 * w, width: "100%", height: "100%"});
        this.$svg.appendChild(setNodeAttributes(createSVGNode('circle', TEMPLATE), {fill:"#fff"}));
        this.$svg.appendChild(setNodeAttributes(createSVGNode('circle', TEMPLATE), {fill:"transparent", stroke:"#d2d3d4", strokeWidth: TEMPLATE.strokeWidth}));
        this.$svg.appendChild(setNodeAttributes(createSVGNode('circle', TEMPLATE), {fill:"transparent", stroke:this.options.color, strokeWidth: TEMPLATE.strokeWidth, strokeDasharray: TEMPLATE.circ * this.options.value + " " + (TEMPLATE.circ * (100 - this.options.value)) }));
        var g = createSVGNode('g', {width: "100", height: "100", style: "font-size: " + 32 * w});
        g.classList.add('chart-text');
        var text = createSVGNode('text', {x:'50%', y:'50%'});
        text.classList.add('chart-number');
        var label = createSVGNode('text', {x:'50%', y:'50%'});
        label.classList.add('chart-label');
        g.appendChild(text);
        g.appendChild(label);
        text.innerHTML=this.options.value + "%";
        label.innerHTML=this.options.text
        this.$svg.appendChild(g);
        this.$html.appendChild( this.$svg );
        this.$wrapper.appendChild( this.$html );
    }
        /*
function getNode(n, v) {
  n = document.createElementNS("http://www.w3.org/2000/svg", n);
  for (var p in v)
    n.setAttributeNS(null, p.replace(/[A-Z]/g, function(m, p, o, s) { return "-" + m.toLowerCase(); }), v[p]);
  return n
}
var svg = getNode("svg");
document.body.appendChild(svg);
var r = getNode('rect', { x: 10, y: 10, width: 100, height: 20, fill:'#ff00ff' });
svg.appendChild(r);
var r = getNode('rect', { x: 20, y: 40, width: 100, height: 40, rx: 8, ry: 8, fill: 'pink', stroke:'purple', strokeWidth:7 });
svg.appendChild(r);
        */

}( document, window ));

// ie9+

( function( Clueschart ) {
    var doc = document,
        initialise = function() {
            var $charts = document.querySelectorAll( "[data-clueschart]" );
            Array.prototype.forEach.call( $charts , function( $el ) {
                var options = JSON.parse( JSON.stringify( $el.dataset ) ),
                    instance = new Clueschart( $el, options );
                $el.dataset.clueschart = instance;
            });
        };
    if (doc.readyState === "complete" || ( doc.readyState !== "loading" && !doc.documentElement.doScroll ) ) {
        initialise();
    } else {
        doc.addEventListener("DOMContentLoaded", initialise );
    }
}( Clueschart ));
