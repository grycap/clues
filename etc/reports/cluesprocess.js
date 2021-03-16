const C_STATE=0x01;
const C_MEMORY_USED=0x02;
const C_MEMORY_AVAIL=0x04;
const C_SLOTS_USED=0x08;
const C_SLOTS_AVAIL=0x10;
const C_DISCRETIZE=0x80;
const C_ALL=0xff;

const colorset_states = {
    "-2": "#ff6f69",
    "-1": "#d2d3d4",
    "0": "#ffcc5c",
    "1": "#696fff",
    "2": "#88d8b0",
    "3": "#ccff5c",
    "4": "#cc5cff",
    "5": "#bb991c",
    "6": "#44a880"
};

const cluesstates = {
    mapping: {
      "-2": "-2",
      "-1": "-1",
      "0": "0",
      "1": "1",
      "2": "2",
      "3": "3",
      "4": "4",
      "5": "0",
      "6": "-2"
    },
    names: {
      "-2": "error",
      "-1": "unknown",
      "0": "idle",
      "1": "used",
      "2": "off",
      "3": "powering on",
      "4": "powering off",
      "5": "on (err)",
      "6": "off (err)"
    },
    draw: ["1", "0", "4", "3", "-1", "-2"],
    avail: ["0", "1", "5"],
    stats: ["-2", "-1", "0", "2"]
};

function sliderChange(chart, sliderName, event, ui) {
    $('#min-' + sliderName).text(moment(ui.values[0]).format('D/M/YYYY HH:mm:ss'));//.text((ui.values[0] / 1000).toFixed(0));
    $('#max-' + sliderName).text(moment(ui.values[1]).format('D/M/YYYY HH:mm:ss'));//.text((ui.values[1] / 1000).toFixed(0));
    chart.options.scales.xAxes[0].time.min = moment(ui.values[0]);
    chart.options.scales.xAxes[0].time.max = moment(ui.values[1]);
    chart.update();
}

function minmax(o) {
    var min = undefined;
    var max = undefined;
    o.forEach(function (v) {
        if (min === undefined) min = v.x;
        if (max === undefined) max = v.x;
        if (v.x < min) min = v.x;
        if (v.x > max) max = v.x;
    });
    return { min: min, max: max };
}

Array.prototype.unique = function() {
    return this.filter((value, index, self) => self.indexOf(value) === index);
}
Array.prototype.strip = function(v = '') {
    return this.filter(value => value != v);
}
Array.prototype.clone = function() {
	return this.slice(0);
};

class Dataset {
    /**
     * 
     * @param {*} dataset The dataset is a multidimensional array of arrays of {x,y} points:
     *      dataset[0] = { (x,y), (x,y) ... }
     *      dataset[1] = { (x,y), (x,y) ... }
     *      ...
     */
    constructor(dataset) {
        this._dataset = dataset;
        this.minimize();
        this.fill();
        // console.log(this._dataset)
    }

    get() {
        return this._dataset;
    }

    _minimize_s(dataset, discretize = false) {
        let f_x = discretize?(x)=>Math.round(x):(x)=>x;
        let l_dataset = dataset.sort((a, b) => a.x - b.x)
        let prev = {x:undefined , y: undefined};
        return l_dataset.map(function(v) {
            let x = f_x(v.x);
            if ((prev.x !== x) && (prev.y !== v.y)) {
                prev.x = x;
                prev.y = v.y;
                return {x: x, y: v.y}
            }
            return null;
        }).filter((x) => x !== null);
    }

    /**
     * This function minimizes the number of points in any dimension of the dataset; it makes that
     *   any x point has a y, but y(x_i-1) <> y(x_i)
     *
     * @param {*} discretize Makes that the x values are integer, by rouning them and setting the 
     *      y value to the greatest float value in the series
     */
    minimize(discretize = false) {
        Object.keys(this._dataset).forEach(function(x) {
            this._dataset[x] = this._minimize_s(this._dataset[x], discretize)
        }.bind(this))
    }

    /**
     * This functions returns the dataset making that every point in x has a value y, for each dataset 
     */
    fill() {
        let dataset = this._dataset;
        let keys = Object.keys(this._dataset);
        let _every_x = keys.reduce(
            function(total, s) {
                return total.concat(dataset[s].map( v => v.x ))
            }, []
        ).unique().sort();

        keys.forEach(function(k) {
            let current_data = dataset[k];
            let new_data = [];

            // The initial value is zero and powered off
            let v = {x:0, y:0};
            let i_existing = 0;
            let v_existing = current_data[i_existing];

            _every_x.forEach(function(x) {
                if ((v_existing !== undefined) && (x >= v_existing.x)) {
                    // The current timestep exists in the host, so we'll store it and get it as a reference
                    new_data.push(v_existing);

                    // Get the next existing value
                    i_existing++;
                    v = {x: v_existing.x, y: v_existing.y};
                    v_existing = current_data[i_existing];
                } else {
                    // The current timestamp does not exist in the host, so fill using the previous values
                    let nv = {x: x, y: v.y};
                    new_data.push(nv);
                }
            })

            dataset[k] = new_data;
        });
        return dataset;
    }
}


class CluesData {

    constructor(originaldata, min_t = 0, max_t = 0, criteria = C_ALL) {
        // First we'll try to minimize the dataset by removing the redundant timesteps
        // if (discretize) criteria |= C_DISCRETIZE;
        //originaldata = this.minimize_data(originaldata, criteria);
        this._cluesdata = originaldata;

        let hostnames = Object.keys(originaldata.hostevents);
        this._hostnames = hostnames;

        // Get the different timesteps
        let _every_t = hostnames.reduce(
            function(total, hostname) {
                return total.concat(originaldata.hostevents[hostname].map( v => v.t ))
            }, []
        ).unique().sort();

        // Grab the minimum and maximum time available
        this._min_t_avail = _every_t[0];
        this._max_t_avail = _every_t[_every_t.length - 1];

        // Adjust the limits
        if ((min_t === undefined) || (min_t < this._min_t_avail)) min_t = this._min_t_avail;
        if ((max_t === undefined) || (max_t <= 0 ) || (max_t > this._max_t_avail)) max_t = this._max_t_avail;

        // Get only the timesteps in the data interval, and remove the outliers
        _every_t = filterOutliers(_every_t.filter((v) => (v >= min_t) && (v <= max_t)));

        // Add the initial and final timesteps according to the period requested by the user; otherwise the results in the interface are weird, because the user cannot see his limits
        //   the values will be set during the "fill in the gaps" phase
        // _every_t.push(min_t);
        // _every_t.push(max_t);
        _every_t = _every_t.sort().unique();
        _every_t[0] = min_t;
        _every_t[_every_t.length - 1] = max_t;

        // Readjust the limits to the actual data
        this._min_t_avail = _every_t[0];
        this._max_t_avail = _every_t[_every_t.length - 1];
        min_t = this._min_t_avail;
        max_t = this._max_t_avail;

        // Now get important information from the dataset
        // Filter the data and sort it
        let max_slots = {};
        let max_memory = {};

        hostnames.forEach(function(_hostname) {
            // Filter the data to have only the interval requested in the function
            originaldata.hostevents[_hostname] = originaldata.hostevents[_hostname].filter(v => ((v.t >= min_t) && (v.t <= max_t))).sort((a,b) => a.t - b.t);

            if (originaldata.hostevents[_hostname].length == 0) {
                // If one node has no activity, let's add the beginning and the end in "unknown" state
                originaldata.hostevents[_hostname].push({"memory_used": 0, "state": -1, "t": min_t, "slots": 0, "slots_used": 0, "memory": 0})
                originaldata.hostevents[_hostname].push({"memory_used": 0, "state": -1, "t": max_t, "slots": 0, "slots_used": 0, "memory": 0})
            }
            // Add min and max points if they do not exist (to avoid weird things when the user requests a specific interval of time)
            originaldata.hostevents[_hostname][0].t = min_t;
            originaldata.hostevents[_hostname][originaldata.hostevents[_hostname].length-1].t = max_t;
            max_slots[_hostname] = originaldata.hostevents[_hostname].map( v => v.slots).sort().pop();
            max_memory[_hostname] = originaldata.hostevents[_hostname].map( v => v.memory).sort().pop();
        })

        // Now get the max slots and memory for each host and in total
        let slots_v = Object.values(max_slots).sort();
        this._slots = {
            hosts: max_slots,
            max: slots_v.pop(),
            total: slots_v.reduce((t, c) => t + c, 0)
        }

        let memory_v = Object.values(max_memory).sort();
        this._memory = {
            hosts: max_memory,
            max: memory_v.pop(),
            total: memory_v.reduce((t, c) => t + c, 0)
        }

        let states_stats = this.calc_states_stats();
        //let requests_stats = this.calc_requests_stats();
        this._states_stats = states_stats.states;
        this._hosts_stats = states_stats.hosts;

        // Now we'll fill the gaps in order to each host to have every timestep
        this._every_t = _every_t;
        this._hostevents = this.fill_the_gaps(originaldata, _every_t);
    }

    calc_requests_stats() {
        let requests_stats = {
            total: cluesdata.requests.length,
            // time_period: to - from,
            size_info: {},
            requests_states: {
              "0": 0, "1": 0, "2": 0, "3": 0, "-1": 0, "-2": 0, "-3": 0, "-4": 0 
            },
            requests_times: {
              accum: {
              "0": 0, "1": 0, "2": 0, "3": 0, "-1": 0, "-2": 0, "-3": 0, "-4": 0 
              },
              max: {
              "0": undefined, "1": undefined, "2": undefined, "3": undefined, "-1": undefined, "-2": undefined, "-3": undefined, "-4": undefined
              },
              min: {
              "0": undefined, "1": undefined, "2": undefined, "3": undefined, "-1": undefined, "-2": undefined, "-3": undefined, "-4": undefined
              },
              in_clues: {
                max: undefined, min: undefined, mean: undefined
              },
              served: {
                max: undefined, min: undefined, mean: undefined
              }
            }
        }
    
        let max_r = {
            min_slots: undefined,
            max_slots: undefined,
            accum_slots: 0,
            min_memory: undefined,
            max_memory: undefined,
            accum_memory: 0,
            min_taskcount: undefined,
            max_taskcount: undefined,
            accum_taskcount: 0,

            // The size is for the charts
            max_size: undefined,
            min_size: undefined,
        }
    
          /*
        PENDING = 0
        ATTENDED = 1        # It is being attended (its resources are being activated)
        SERVED = 2          # The request has been fully attended: its resources have been powered on and the request has been freed
        BLOCKED = 3         # The request could be served, but the scheduler has blocked it (maybe because there are other requests pending from powering on resources)
        DISSAPEARED = -1    # The request has dissapeared: it was observed (i.e. a job) and now it cannot be observed, but it is not kown if it has been served
        NOT_SERVED = -2
        DISCARDED = -3       # The request has been discarded by the server, probably because it has been attended too many times without success
        UNKNOWN = -4
          */
    
        // We gather the data to get the max and min values
        cluesdata.requests.forEach(function(r) {
            if (max_r.min_slots == undefined || r.slots < max_r.min_slots) max_r.min_slots = r.slots;
            if (max_r.max_slots == undefined || r.slots > max_r.max_slots) max_r.max_slots = r.slots;
            if (max_r.min_memory == undefined || r.memory < max_r.min_memory) max_r.min_memory = r.memory;
            if (max_r.max_memory == undefined || r.memory > max_r.max_memory) max_r.max_memory = r.memory;
            if (max_r.min_taskcount == undefined || r.taskcount < max_r.min_taskcount) max_r.min_taskcount = r.taskcount;
            if (max_r.max_taskcount == undefined || r.taskcount > max_r.max_taskcount) max_r.max_taskcount = r.taskcount;
    
            // TODO: revise: should we accumulate the total amount of memory or not?
            max_r.accum_memory += r.memory * r.taskcount;
            max_r.accum_slots += r.slots * r.taskcount;
            max_r.accum_taskcount += r.taskcount;
    
            if (r.state !== undefined && r.state !== null && r.state != 'null') {
              requests_stats.requests_states[r.state]++;
    
              // Consider only the current interval
              if (r.t_state < from) r.t_state = from;
              if (r.t_state > to) r.t_state = to;
              if (r.t_created < from) r.t_created = from;
              if (r.t_created > to) r.t_created = to;
    
              let req_time = r.t_state - r.t_created;
              requests_stats.requests_times.accum[r.state]+= req_time;
              if (requests_stats.requests_times.max[r.state] == undefined || req_time > requests_stats.requests_times.max[r.state]) 
                requests_stats.requests_times.max[r.state] = req_time;
              if (requests_stats.requests_times.min[r.state] == undefined || req_time < requests_stats.requests_times.min[r.state]) 
                requests_stats.requests_times.min[r.state] = req_time;
            }
        }.bind(this))
    
        // Will get the max, min and mean in CLUES and to be served

        const STATES=["0", "1", "2", "3", "-1", "-2", "-3", "-4" ];
        const STATES_SERVED=["2"];

        function get_stats(requests_stats, STATES) {
            var total = 0;
            var req_times = {
                mean: 0,
                max: undefined,
                min: undefined,
            };
            STATES.forEach(function (s) {
                if (requests_stats.requests_states[s] != 0) {
                    req_times.mean += requests_stats.requests_times.accum[s];
                    total += requests_stats.requests_states[s];
                    if (req_times.max == undefined || requests_stats.requests_times.max[s] > req_times.max)
                    req_times.max = requests_stats.requests_times.max[s];
                    if (req_times.min == undefined || requests_stats.requests_times.min[s] < req_times.min)
                    req_times.min = requests_stats.requests_times.min[s];
                }
            })
            if (total > 0) {
                req_times.mean = (req_times.mean / total).toFixed(3);
                req_times.max = req_times.max.toFixed(3);
                req_times.min = req_times.min.toFixed(3);
            } else {
                req_times.mean = "Not avail";
                req_times.max = "Not avail";
                req_times.min = "Not avail";
            }
            return req_times;
        }
        requests_stats.requests_times.in_clues = get_stats(requests_stats, STATES);
        requests_stats.requests_times.served = get_stats(requests_stats, STATES_SERVED);
    
        requests_stats.size_info = max_r;
    }

    /**
     * This function fills the dataset to make that each dimension has the same timesteps. This is of special interest to easier build graphs
     * @param {*} originaldata 
     * @param {*} _every_t 
     */
    fill_the_gaps(originaldata, _every_t) {
        let _hostevents = [];

        this._hostnames.forEach(function(_hostname) {
            let current_data = originaldata.hostevents[_hostname];
            let new_host_data = [];

            // The initial value is zero and powered off
            let v = {"memory_used": 0, "state": 2, "t": 0, "slots": 0, "slots_used": 0, "memory": 0};
            let v_s = JSON.stringify(v);

            let i_host = 0;
            let v_host = current_data[i_host];

            _every_t.forEach(function(t) {
                if ((v_host !== undefined) && (t >= v_host.t)) {
                    // The current timestep exists in the host, so we'll store it and get it as a reference
                    new_host_data.push(v_host);

                    // Get the next existing value
                    i_host++;
                    v_s = JSON.stringify(v_host);
                    v_host = current_data[i_host];
                } else {
                    // The current timestamp does not exist in the host, so fill using the previous values
                    let nv = JSON.parse(v_s); nv.t = t;
                    new_host_data.push(nv);
                }
            })

            _hostevents[_hostname] = new_host_data;
        });

        return _hostevents;
    }

    /**
     * This function tries to reduce the number of points in the dataset while keeping the information.
     *  - Removes redundant point (i.e. same data in different timesteps)
     *      + Enable different criteria; i.e. keeping state, keeping used slots, keeping memory used, etc.
     *  - Discretize time (i.e. make that every point is integer by rounding to the nearest integer time)
     * @param {*} originaldata 
     * @param {*} criteria 
     */
    minimize_data(originaldata, criteria = C_ALL) {
        let hostnames = Object.keys(originaldata.hostevents);

        /*
        // Get the different timesteps
        let _every_t = hostnames.reduce(
            function(total, hostname) {
                return total.concat(originaldata.hostevents[hostname].map( v => v.t ))
            }, []
        ).unique().sort();
        */

        let check_STATE = (criteria & C_STATE) == C_STATE;
        let check_MEMORY_USED = (criteria & C_MEMORY_USED) == C_MEMORY_USED;
        let check_SLOTS_USED = (criteria & C_SLOTS_USED) == C_SLOTS_USED;
        let check_MEMORY_AVAIL = (criteria & C_MEMORY_AVAIL) == C_MEMORY_AVAIL;
        let check_SLOTS_AVAIL = (criteria & C_SLOTS_AVAIL) == C_SLOTS_AVAIL;
        let discretize = (criteria & C_DISCRETIZE) == C_DISCRETIZE;

        let f_t =discretize?(x)=>Math.round(x):(x)=>x;
        function equal(a, b) {
            if ((a === undefined) || (b === undefined)) return false;
            if (check_STATE && (a.state != b.state)) return false;
            if (check_MEMORY_USED && (a.memory_used != b.memory_used)) return false;
            if (check_SLOTS_USED && (a.slots_used != b.slots_used)) return false;
            if (check_MEMORY_AVAIL && (a.memory != b.memory)) return false;
            if (check_SLOTS_AVAIL && (a.slots != b.slots)) return false;
            return true;
        }

        hostnames.forEach(function(_hostname) {
            let hostevents = originaldata.hostevents[_hostname].sort((a,b) => a.t - b.t);
            let t_hostevents = {};

            // Pass the events to a dict, to make sure of the uniqueness
            let prev = undefined;
            let skipped = 0;
            hostevents.forEach(function(e) {
                e.t = f_t(e.t);
                if (! equal(e, prev)) {
                    t_hostevents[e.t] = e;
                    prev = JSON.parse(JSON.stringify(e));
                } else
                    skipped++;
            })

            console.log('skipped', skipped, 'entries');

            let new_hostevents = [];
            for (let t in t_hostevents)
                new_hostevents.push(t_hostevents[t])

            originaldata.hostevents[_hostname] = new_hostevents;
        });

        return originaldata;
    }    

    calc_host_state_change_series() {
        let available_states = Object.keys(cluesstates.mapping);
        let series = {};

        // Initialize the series; where series[s] will be the time series for state s
        available_states.forEach(function(s) {
            series[s] = {};
            this._every_t.forEach(function(t) { 
                series[s][t] = 0;
            });
        }.bind(this));

        let _hostevents = this._hostevents;

        // Now get any host event and accumm to the state in that time step
        this._hostnames.forEach(function(_hostname) {
            _hostevents[_hostname].forEach(function (v) {
                let state = cluesstates.mapping[v.state];
                series[state][v.t]++;
            })
        });

        let data = [];
        for (let s in series) {
            //console.log(series[s]);
            // let statename = cluesstates.names[s];
            let c_data = [];
            for (let t in series[s]) {
                //console.log(t, series[s][t]);
                c_data.push({x:parseInt(t * 1000), y:series[s][t]});
            }
            //console.log(c_data);
            data[s] = c_data.sort((a,b) => a.x - b.x);
        };
        return new Dataset(data);
    }

    calc_memory_series() {
        let dataset = {}
        let _hostevents = this._hostevents;

        // Now get any host event and accumm to the state in that time step
        let d_total = {};
        let d_used = {};
        this._hostnames.forEach(function(_hostname) {
            d_total[_hostname] = _hostevents[_hostname].map(v => { return { x: v.t * 1000, y: (cluesstates.avail.indexOf(v.state.toString()) >= 0 ? v.memory : 0) } });
            d_used[_hostname] = _hostevents[_hostname].map(v => { return { x: v.t * 1000, y: v.memory_used } });
        });
        return { total: new Dataset(d_total), used: new Dataset(d_used) };
    }

    calc_slots_series() {
        let dataset = {}
        let _hostevents = this._hostevents;

        // Now get any host event and accumm to the state in that time step
        let d_total = {};
        let d_used = {};        
        this._hostnames.forEach(function(_hostname) {
            d_total[_hostname] = _hostevents[_hostname].map(v => { return { x: v.t * 1000, y: (cluesstates.avail.indexOf(v.state.toString()) >= 0 ? v.slots : 0) } });
            d_used[_hostname] = _hostevents[_hostname].map(v => { return { x: v.t * 1000, y: v.slots_used } });
        });
        return { total: new Dataset(d_total), used: new Dataset(d_used) };
    }

    calc_states_stats() {
        let states_stats = {};
        let hosts_stats = {};

        this._hostnames.forEach(function(hostname) {
            // Now we'll calculate the individual stats for each host
            states_stats[hostname] = new StatesStats(hostname, (current_state, current_time) => cluesstates.avail.indexOf(current_state.state.toString()) >= 0);
            states_stats[hostname].get_from_hostdata(cluesdata.hostevents[hostname]);
            for (let s in cluesstates.names) {
                // Complete the stats, to avoid undefineds in later usage of the stats
                if (states_stats[hostname]._stats_state[s] == undefined) {
                    states_stats[hostname]._stats_state[s] = 0;
                    states_stats[hostname]._stats_time[s] = 0;
                    states_stats[hostname]._stats_pct_time[s] = 0;
                    states_stats[hostname]._stats_pct_time_total[s] = 0;
                }
            }
            hosts_stats[hostname] = {
                memory_used: new AreaStats(cluesdata.hostevents[hostname].map((v) => ({ x: v.t, y: v.memory_used, state: v.state })), (a, b) => (a.y == b.y && a.state == b.state), this._memory.max, (current, x) => cluesstates.avail.indexOf(current.state.toString()) >= 0),
                slots_used: new AreaStats(cluesdata.hostevents[hostname].map((v) => ({ x: v.t, y: v.slots_used, state: v.state })), (a, b) => (a.y == b.y && a.state == b.state), this._slots.max, (current, x) => cluesstates.avail.indexOf(current.state.toString()) >= 0)
            }
        }.bind(this))

        return { states: states_stats, hosts: hosts_stats };
    }
}


class CluesGraphs {
    constructor(parseddata) {
        this._cluesdata = parseddata;

        var color_array = randomColor({ count: parseddata._hostnames.length, format: 'rgba', alpha: 0.8, luminosity: 'bright' });
        this.colorset = {};
        parseddata._hostnames.forEach(function(_hostname, i) {
            this.colorset[_hostname] = color_array[i];
        }.bind(this));
    }

    draw_states_changes() {
        
        // Calculate the datasets to add to the chart (one dataset per state to be drawn)
        let dataset = [];
        let states_series = this._cluesdata.calc_host_state_change_series().get();

        cluesstates.draw.forEach(function (s) {
            let statename = cluesstates.names[s];
            let data = states_series[s];

            let current_state_dataset = {
                label: ["Slots in " + statename],
                data: data,
                steppedLine: true,
                spanGaps: true,
                pointRadius: 0,
                borderWidth: 1,
                backgroundColor: colorset_states[s],
                borderColor: colorset_states[s],
            }

            dataset.push(current_state_dataset);            
        });

        // Create a chart for the change of states
        let chartStates = StackedLineChart(
            "chartStates",
            [].concat(dataset),
            ["states"],
            (e, legendItem) => onClick_cohide(e, legendItem, chartStates)
        )
        chartStates.update();

        // Adjust the slider to the actual data
        var minmax_states = minmax(chartStates.data.datasets[0].data);
        $("#chartStatesSlider").slider({
            range: true,
            min: minmax_states.min,
            max: minmax_states.max,
            values: [minmax_states.min, minmax_states.max],
            step: 1,
            create: (event, ui) => sliderChange(chartStates, "chartStatesSlider", event, { values: [minmax_states.min, minmax_states.max] }),
            slide: (event, ui) => sliderChange(chartStates, "chartStatesSlider", event, ui)
        });    
    }

    draw_memory_used() {
        // Calculate the datasets to add to the chart (one dataset per state to be drawn)
        let dataset = [];
        let memory_series = this._cluesdata.calc_memory_series();
        let memory_total = memory_series.total.get();
        let memory_used = memory_series.used.get();
        let hostnames = Object.keys(memory_total);
        let colorset = this.colorset;

        hostnames.forEach(function(_hostname) {
            let current_used = {
                label: ["Memory used in " + _hostname],
                data: memory_used[_hostname],
                steppedLine: true,
                spanGaps: false,
                pointRadius: 0,
                yAxisID: "memory_used",
                backgroundColor: colorset[_hostname]    
            }
            dataset.push(current_used);
        })

        hostnames.forEach(function(_hostname) {
            let current_total = {
                label: ["Memory in " + _hostname],
                data: memory_total[_hostname],
                steppedLine: true,
                spanGaps: false,
                pointRadius: 0,
                yAxisID: "memory",
                backgroundColor: "rgba(150,150,150,1)",
                borderColor: "rgba(150,150,150,1)",
            }            
            dataset.push(current_total);
        })

        let chartMemory = StackedLineChart(
            "chartMemory",
            [].concat(dataset),
            ["memory", "memory_used"],
            (e, legendItem) => onClick_cohide(e, legendItem, chartMemory, "Memory used in "),
            (item, chart) => (item.text[0].substring(0, 9) == "Memory in") ? null : item
        )

        chartMemory.options.scales.yAxes[1].display = false;
        chartMemory.options.scales.yAxes[0].ticks.max = this._cluesdata._total_memory;
        chartMemory.options.scales.yAxes[1].ticks.max = this._cluesdata._total_memory;
        chartMemory.update();

        var minmax_memory = { min: undefined, max: undefined }
        if (chartMemory.data.datasets[chartMemory.data.datasets.length - 1] !== undefined) 
            minmax_memory = minmax(chartMemory.data.datasets[chartMemory.data.datasets.length - 1].data);
        $("#chartMemorySlider").slider({
            range: true,
            min: minmax_memory.min,
            max: minmax_memory.max,
            tooltips: [true, true],
            values: [minmax_memory.min, minmax_memory.max],
            step: 1,
            create: (event, ui) => sliderChange(chartMemory, "chartMemorySlider", event, { values: [minmax_memory.min, minmax_memory.max] }),
            slide: (event, ui) => sliderChange(chartMemory, "chartMemorySlider", event, ui)
        });
    }    

    draw_slots_used() {
        // Calculate the datasets to add to the chart (one dataset per state to be drawn)
        let dataset = [];
        let slots_series = this._cluesdata.calc_slots_series()
        let slots_total = slots_series.total.get();
        let slots_used = slots_series.used.get();

        let hostnames = Object.keys(slots_total);
        let colorset = this.colorset;

        // The order is important... the first has more priority to be shown, so
        //   we want to have the used slots in first plane
        hostnames.forEach(function(_hostname) {
            let current_used = {
                label: ["Slots used in " + _hostname],
                data: slots_used[_hostname],
                steppedLine: true,
                spanGaps: false,
                pointRadius: 0,
                yAxisID: "slots_used",
                backgroundColor: colorset[_hostname]    
            }
            dataset.push(current_used);
        })

        // The background is for the available hosts
        hostnames.forEach(function(_hostname) {
            let current_total = {
                label: ["Slots in " + _hostname],
                data: slots_total[_hostname],
                steppedLine: true,
                spanGaps: false,
                pointRadius: 0,
                yAxisID: "slots",
                backgroundColor: "rgba(150,150,150,1)",
                borderColor: "rgba(150,150,150,1)",
            }            
            dataset.push(current_total);
        })

        let chartSlots = StackedLineChart(
            "chartSlots",
            [].concat(dataset),
            ["slots", "slots_used"],
            (e, legendItem) => onClick_cohide(e, legendItem, chartSlots, "Slots used in "),
            (item, chart) => (item.text[0].substring(0, 8) == "Slots in") ? null : item
        )
        
        chartSlots.options.scales.yAxes[1].display = false;
        chartSlots.options.scales.yAxes[0].ticks.max = this._cluesdata._total_slots;
        chartSlots.options.scales.yAxes[1].ticks.max = this._cluesdata._total_slots;
        chartSlots.update();

        var minmax_slots = { min: undefined, max: undefined }
        if (chartSlots.data.datasets[chartSlots.data.datasets.length-1] !== undefined) 
        minmax_slots = minmax(chartSlots.data.datasets[chartSlots.data.datasets.length-1].data);
        $("#chartSlotsSlider").slider({
            range: true,
            min: minmax_slots.min,
            max: minmax_slots.max,
            tooltips: [true, true],
            values: [minmax_slots.min, minmax_slots.max],
            step: 1,
            create: (event, ui) => sliderChange(chartSlots, "chartSlotsSlider", event, { values: [minmax_slots.min, minmax_slots.max] }),
            slide: (event, ui) => sliderChange(chartSlots, "chartSlotsSlider", event, ui)
        });        
    }    
}

var cd;
function preprocess() {
    cd = new CluesData(cluesdata);

    app.hostnames = cd._hostnames.sort();
    app.states_stats = cd._states_stats;
    app.hosts_stats = cd._hosts_stats;

    let g = new CluesGraphs(cd);
    g.draw_states_changes();
    g.draw_memory_used();
    g.draw_slots_used();

    return true;
}
