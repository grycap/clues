function StatesStats(hostname, accept_interval = function(current_state, current_time) {return true}) {
  this.hostname = hostname;
  this._current_state = undefined;
  this._stats_state = undefined;
  this._stats_time = undefined;
  this._start_time = undefined;
  this._end_time = undefined;
  this._time_avail = 0;
  this._stats_pct_time = undefined;
  this._stats_pct_time_total = undefined;
  if (accept_interval===undefined || accept_interval==null)
    this._accept_interval = (current_state, current_time) => true;
  else
    this._accept_interval = accept_interval;

  this.begin_serie = function() {
    this._current_state = undefined;

    this._stats_state = [];
    this._stats_time = [];
    this._stats_pct_time = [];
    this._stats_pct_time_total = [];

    this._start_time = undefined;
    this._end_time = undefined;
    this._time_avail = 0;
  }

  this.add_data = function(data) {
    if (this._start_time === undefined) {
      this._start_time = data.t;
    }
    if (this._current_state === undefined) {
      this._current_state = {
        state: data.state,
        time: data.t,
      }
    }
    if (this._current_state.state != data.state) {
      if (this._stats_state[this._current_state.state] == undefined)
        this._stats_state[this._current_state.state] = 0;
      if (this._stats_time[this._current_state.state] == undefined)
        this._stats_time[this._current_state.state] = 0;

      this._stats_state[this._current_state.state]++;
      this._stats_time[this._current_state.state] += data.t - this._current_state.time;
      if (this._accept_interval(this._current_state)) 
        this._time_avail += data.t - this._current_state.time;

      this._current_state = {
        state: data.state,
        time: data.t,
      };
    }
    this._end_time = data.t;
  }
  this.end_serie = function() {
    if (this._accept_interval(this._current_state))
      this._time_avail += this._end_time - this._current_state.time;

    if (this._stats_time[this._current_state.state] == undefined) 
      this._stats_time[this._current_state.state] = 0;

    this._stats_time[this._current_state.state] += this._end_time - this._current_state.time;            
    var total_time = this._end_time - this._start_time;
    for (s in this._stats_time) {
      this._stats_pct_time[s] = Math.round(1000.0 * this._stats_time[s] / this._time_avail)/10.0;
      this._stats_pct_time_total[s] = Math.round(1000.0 * this._stats_time[s] / total_time)/10.0;
    }
  }

  this.get_from_hostdata = function(hostdata) {
    this.begin_serie();
    hostdata.forEach((data) => this.add_data(data));
    this.end_serie();
  }
  return this;
}

function filterOutliers(someArray) {
  // Filter out values using the interquartile test (https://en.wikipedia.org/wiki/Interquartile_range)
  // Implementation from:
  //  https://stackoverflow.com/a/45804710

  if(someArray.length < 4)
      return someArray;

  let values, q1, q3, iqr, maxValue, minValue;

  values = someArray.slice().sort( (a, b) => a - b);//copy array fast and sort

  if((values.length / 4) % 1 === 0){//find quartiles
    q1 = 1/2 * (values[(values.length / 4)] + values[(values.length / 4) + 1]);
    q3 = 1/2 * (values[(values.length * (3 / 4))] + values[(values.length * (3 / 4)) + 1]);
  } else {
    q1 = values[Math.floor(values.length / 4 + 1)];
    q3 = values[Math.ceil(values.length * (3 / 4) + 1)];
  }

  iqr = q3 - q1;
  maxValue = q3 + iqr * 1.5;
  minValue = q1 - iqr * 1.5;

  return values.filter((x) => (x >= minValue) && (x <= maxValue));
}

const INTERVALS=4.0;

const AreaStats = function(dataset, equality_fnc = function (a,b) { return a == b }, max, accept_interval = function(current_value, current_x) {return true}) {

  // https://math.stackexchange.com/questions/102978/incremental-computation-of-standard-deviation
  // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance

  this.max = max;
  this._divider = max / INTERVALS;
  if (accept_interval===undefined || accept_interval==null)
    this._accept_interval = (current_value, current_x) => true;
  else
    this._accept_interval = accept_interval;
  this._equal_fnc = equality_fnc;

  this.begin_serie = function() {
    this.area = 0;
    this.current_value = undefined;
    this.start_x = undefined;
    this.mean = 0;
    this.mean_accepted = 0;
    this.intervals = [];
    this.accepted_x = 0;
    for (var i = -1; i< INTERVALS; i++)
      this.intervals[i] = 0;
  }

  this.add_data = function(data) {

    if (data.y > this.max)
      throw data.y + " is greather than the max expected: " + this.max;

    if (this.start_x === undefined)
      this.start_x = data.x;

    if (this.current_value === undefined)
      this.current_value = data;

    if (! this._equal_fnc(this.current_value, data)) {
      if (this._accept_interval(this.current_value, data.x)) {
        this.accepted_x += (data.x - this.current_value.x)
        this.area += (data.x - this.current_value.x) * this.current_value.y;

        if (this._divider != 0) {
          var category = Math.ceil( data.y / this._divider) - 1;
          this.intervals[category] += (data.x - this.current_value.x);
        }
      }

      this.current_value = data;
    }
    this.end_x = data.x;
  }

  this.end_serie = function() {
    if (this._accept_interval(this.current_value, this.end_x)) {
      this.accepted_x += (this.end_x - this.current_value.x)
      this.area += (this.end_x - this.current_value.x) * this.current_value.y;

      if (this._divider != 0) {
        var category = Math.ceil( this.current_value.y / this._divider);
        this.intervals[category] += (this.end_x - this.current_value.x);
      }
    }

    this.mean = this.area / parseFloat(this.end_x - this.start_x);
    this.mean_accepted = this.area / parseFloat(this.accepted_x);
  }

  this.get_from_dataset = function(dataset) {
    this.begin_serie();
    dataset.forEach((data) => data?this.add_data(data):null);
    this.end_serie();
  }

  this.get_from_dataset(dataset);
  return this;
}
